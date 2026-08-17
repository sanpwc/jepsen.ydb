(ns jepsen.ydb.append-single-row
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable])
  (:import (java.util ArrayList)
           (com.google.protobuf ByteString)
           (tech.ydb.core StatusCode)
           (tech.ydb.table.query Params)
           (tech.ydb.table.values PrimitiveValue)
           (tech.ydb.table.values Value)))

(def ^:dynamic *ballast* (ByteString/copyFromUtf8 ""))

(defn new-ballast
  "Creates a new ballast value with size bytes"
  [size]
  (ByteString/copyFromUtf8 (.repeat "x" size)))

(defmacro with-ballast
  "Runs body with *ballast* bound to the specified ballast value"
  [ballast & body]
  `(binding [*ballast* ~ballast]
     (do ~@body)))

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(defn generate-partition-at-keys
  "Generates an optional PARTITION_AT_KEYS fragment with a list of partitioning keys"
  [test]
  (let [keys (:initial-partition-keys test)
        count (:initial-partition-count test)]
    (if (> count 1)
      (format "PARTITION_AT_KEYS = (%s),"
              (->> (iterate inc 1)
                   (map #(+ (* % keys) 1))
                   (take (dec count))
                   (str/join ", ")))
      "")))

(defn generate-read-replicas-settings
  "Generates an optional READ_REPLICAS_SETTINGS fragment"
  [test]
  (let [count (:with-read-replicas test)]
    (if (> count 0)
      (format "READ_REPLICAS_SETTINGS = \"PER_AZ:%s\"," count)
      "")))

(defn drop-initial-tables
  [test query-client]
  (info "dropping initial tables")
  (conn/with-session [session query-client]
    (let [query (format "DROP TABLE IF EXISTS `%1$s`;" (:db-table test))]
      (conn/execute-scheme! session query))))

(defn create-initial-tables
  [test query-client]
  (info "creating initial tables")
  (conn/with-session [session query-client]
    (let [query (format "CREATE TABLE `%1$s` (
                             key Int64 NOT NULL,
                             val String,
                             ballast Bytes,
                             PRIMARY KEY (key))
                         WITH (%3$s
                               %4$s
                               STORE = %5$s,
                               AUTO_PARTITIONING_BY_SIZE = ENABLED,
                               AUTO_PARTITIONING_BY_LOAD = ENABLED,
                               AUTO_PARTITIONING_PARTITION_SIZE_MB = %2$d);"
                        (:db-table test)
                        (:partition-size-mb test)
                        (generate-partition-at-keys test)
                        (generate-read-replicas-settings test)
                        (:store-type test))]
      (conn/execute-scheme! session query))))

(defn execute-key-exists
  "Returns true if the given key exists in the table."
  [test tx k]
  (let [query (format "DECLARE $key AS Int64;
                       SELECT key FROM `%s` WHERE key = $key;"
                      (:db-table test))
        params (Params/of "$key" (PrimitiveValue/newInt64 k))
        result (conn/execute! tx query params)
        rs (.getResultSet result 0)]
    (.next rs)))

(defn execute-list-read
  "Reads the list for key k. Returns nil if absent, or a vector of longs."
  [test tx k]
  (let [query (format "DECLARE $key AS Int64;
                       SELECT val FROM `%s` WHERE key = $key;"
                      (:db-table test))
        params (Params/of "$key" (PrimitiveValue/newInt64 k))
        result (conn/execute! tx query params)
        rs (.getResultSet result 0)]
    (if (.next rs)
      (let [val-str (String. (-> rs (.getColumn 0) .getBytes))]
        (if (or (nil? val-str) (str/blank? val-str))
          []
          (mapv #(Long/parseLong %) (str/split val-str #","))))
      nil)))

(defn execute-list-insert
  "Inserts a new row for key k with val as the first element."
  [test tx k v]
  (let [query (format "DECLARE $key AS Int64;
                       DECLARE $value AS Int64;
                       DECLARE $ballast AS Bytes;
                       INSERT INTO `%s` (key, val, ballast) VALUES ($key, CAST($value AS String), $ballast);"
                      (:db-table test))
        params (Params/of "$key"     (PrimitiveValue/newInt64 k)
                          "$value"   (PrimitiveValue/newInt64 v)
                          "$ballast" (PrimitiveValue/newBytes *ballast*))]
    (conn/execute! tx query params)))

(defn execute-list-update
  "Appends v to the existing list for key k via server-side concatenation."
  [test tx k v]
  (let [query (format "DECLARE $key AS Int64;
                       DECLARE $value AS Int64;
                       DECLARE $ballast AS Bytes;
                       UPDATE `%s` SET val = val || ',' || CAST($value AS String), ballast = $ballast
                         WHERE key = $key;"
                      (:db-table test))
        params (Params/of "$key"     (PrimitiveValue/newInt64 k)
                          "$value"   (PrimitiveValue/newInt64 v)
                          "$ballast" (PrimitiveValue/newBytes *ballast*))]
    (conn/execute! tx query params)))

(defn batch-commit-last
  "Wraps the last micro op into [:commit nil mop] based on configured probability."
  ([test]
   (let [probability (:batch-commit-probability test 1.0)]
     (fn [rf]
       (let [last (volatile! ::none)]
         (fn
           ([] (rf))
           ([result]
            (let [final @last
                  _ (vreset! last ::none)
                  result (if (identical? final ::none)
                           result
                           (let [final (if (< (rand) probability)
                                         [:commit nil final]
                                         final)]
                             (unreduced (rf result final))))]
              (rf result)))
           ([result op]
            (let [prev @last
                  _ (vreset! last ::none)]
              (if (identical? prev ::none)
                (do
                  (vreset! last op)
                  result)
                (let [result (rf result prev)]
                  (when-not (reduced? result)
                    (vreset! last op))
                  result)))))))))
  ([test coll]
   (sequence (batch-commit-last test) coll)))

(defn apply-mop!
  [test tx [f k v :as mop]]
  (case f
    :r [[f k (execute-list-read test tx k)]]
    :append [(let [exists? (execute-key-exists test tx k)]
               (if exists?
                 (execute-list-update test tx k v)
                 (execute-list-insert test tx k v))
               mop)]
    :commit (do
              (conn/auto-commit! tx)
              (apply-mop! test tx v))))

(defrecord Client [transport query-client ballast setup?]
  client/Client
  (open! [this test node]
    (let [transport (conn/open-transport test node)
          query-client (conn/open-query-client transport)]
      (assoc this :transport transport :query-client query-client)))

  (setup! [this test]
    (once-per-cluster
     setup?
     (drop-initial-tables test query-client)
     (create-initial-tables test query-client)))

  (invoke! [_ test op]
    (with-ballast ballast
      (debug-info/with-debug-info
        (conn/with-errors op
          (conn/with-session [session query-client]
            (conn/with-transaction [tx [session (:model test)]]
              (let [txn (:value op)
                    txn' (->> txn
                              (batch-commit-last test)
                              (into []))
                    op' (if (not= txn txn') (assoc op :modified-txn txn') op)
                    txn'' (->> txn'
                               (mapcat (partial apply-mop! test tx))
                               (into []))
                    op'' (assoc op' :type :ok, :value txn'')]
                op'')))))))

  (teardown! [this test])

  (close! [this test]
    (.close query-client)
    (.close transport)))

(defn new-client
  [opts]
  (Client. nil nil (new-ballast (:ballast-size opts)) (atom false)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (append/test (assoc (select-keys opts [:key-count
                                              :min-txn-length
                                              :max-txn-length
                                              :max-writes-per-key])
                           :consistency-models [(:model opts)])))
      (assoc :client (new-client opts))))
