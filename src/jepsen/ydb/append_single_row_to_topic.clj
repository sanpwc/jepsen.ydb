(ns jepsen.ydb.append-single-row-to-topic
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable])
  (:import (java.nio.charset StandardCharsets)
           (java.util.concurrent TimeUnit)
           (tech.ydb.core StatusCode)
           (tech.ydb.core UnexpectedResultException)
           (tech.ydb.topic.settings DescribeTopicSettings
                                     ReaderSettings
                                     SendSettings
                                     TopicReadSettings
                                     WriterSettings)
           (tech.ydb.topic.write Message)))

;; A YDB topic-only analogue of append-single-row: each Jepsen key is mapped
;; to a single, fixed partition of one shared topic. :append writes a message
;; to that partition inside the surrounding YDB transaction (topic writes
;; require an already-active tx handle, see conn/ensure-tx!). :r performs a
;; full non-destructive replay read of the partition (offset 0..end via a
;; consumer-less reader) rather than tracking a committed offset, since own
;; writes aren't visible within the same transaction anyway and we want reads
;; to be repeatable and side-effect free.

(def read-timeout-ms
  "How long a single replay read waits for a message that DescribeTopic
   already reported as durably written before treating it as an error."
  30000)

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(defn drop-initial-topic
  "Drops the test topic, ignoring the error when it doesn't exist yet."
  [test query-client]
  (info "dropping initial topic")
  (conn/with-session [session query-client]
    (try
      (conn/execute-scheme! session (format "DROP TOPIC `%1$s`;" (:topic-name test)))
      (catch UnexpectedResultException e
        (when-not (= (-> e .getStatus .getCode) StatusCode/SCHEME_ERROR)
          (throw e))))))

(defn create-initial-topic
  "Creates the test topic with a fixed number of partitions equal to
   key-count, one partition per Jepsen key. Auto partitioning is left
   disabled (the default) so partition ids stay stable for the whole test."
  [test query-client]
  (info "creating initial topic")
  (conn/with-session [session query-client]
    (let [query (format "CREATE TOPIC `%1$s` WITH (min_active_partitions = %2$d, max_active_partitions = %2$d);"
                        (:topic-name test) (:key-count test))]
      (conn/execute-scheme! session query))))

(defn get-writer!
  "Returns the cached SyncWriter for partition k, creating and initializing
   one on first use."
  [test topic-client writers k]
  (if-let [w (get @writers k)]
    w
    (let [w (-> topic-client
                (.createSyncWriter (-> (WriterSettings/newBuilder)
                                       (.setTopicPath (:topic-name test))
                                       (.setPartitionId (long k))
                                       .build)))]
      (.initAndWait w)
      (swap! writers assoc k w)
      w)))

(defn execute-topic-append!
  "Appends v to the partition for key k, inside tx."
  [test tx topic-client writers k v]
  (let [raw-tx (conn/ensure-tx! tx)
        writer (get-writer! test topic-client writers k)
        message (Message/of (.getBytes (str v) StandardCharsets/UTF_8))
        settings (-> (SendSettings/newBuilder)
                     (.setTransaction raw-tx)
                     .build)]
    (.send writer message settings)))

(defn execute-topic-read!
  "Reads the partition for key k via a full non-destructive replay: describes
   the topic to find the current [start, end) offset range for the
   partition, then reads exactly that many messages with a fresh,
   consumer-less reader. Returns nil if the partition has never been written
   to, or a vector of longs otherwise."
  [test topic-client k]
  (let [describe-settings (-> (DescribeTopicSettings/newBuilder)
                               (.withIncludeStats true)
                               .build)
        description (-> topic-client
                        (.describeTopic (:topic-name test) describe-settings)
                        .join .getValue)
        partition (->> (.getPartitions description)
                       (filter #(= (.getPartitionId %) (long k)))
                       first)
        offsets (-> partition .getPartitionStats .getPartitionOffsets)
        start (.getStart offsets)
        end (.getEnd offsets)]
    (when (> end start)
      (let [reader-settings (-> (ReaderSettings/newBuilder)
                                 .withoutConsumer
                                 (.addTopic (-> (TopicReadSettings/newBuilder)
                                               (.setPath (:topic-name test))
                                               (.setPartitionIds [(long k)])
                                               .build))
                                 .build)
            reader (.createSyncReader topic-client reader-settings)]
        (try
          (.initAndWait reader)
          (mapv (fn [_]
                  (let [msg (.receive reader read-timeout-ms TimeUnit/MILLISECONDS)]
                    (when (nil? msg)
                      (throw (ex-info "topic replay read timed out"
                                      {:topic (:topic-name test) :key k})))
                    (Long/parseLong (String. (.getData msg) StandardCharsets/UTF_8))))
                (range (- end start)))
          (finally
            (.shutdown reader)))))))

(defn apply-mop!
  [test tx topic-client writers [f k v :as mop]]
  (case f
    :r [[f k (execute-topic-read! test topic-client k)]]
    :append [(do (execute-topic-append! test tx topic-client writers k v) mop)]))

(defrecord Client [transport query-client topic-client writers setup?]
  client/Client
  (open! [this test node]
    (let [transport (conn/open-transport test node)
          query-client (conn/open-query-client transport)
          topic-client (conn/open-topic-client transport)]
      (assoc this :transport transport :query-client query-client :topic-client topic-client)))

  (setup! [this test]
    (once-per-cluster
     setup?
     (drop-initial-topic test query-client)
     (create-initial-topic test query-client)))

  (invoke! [_ test op]
    (debug-info/with-debug-info
      (conn/with-errors op
        (conn/with-session [session query-client]
          (conn/with-transaction [tx [session (:model test)]]
            (let [txn (:value op)
                  txn' (->> txn
                            (mapcat (partial apply-mop! test tx topic-client writers))
                            (into []))]
              (assoc op :type :ok, :value txn')))))))

  (teardown! [this test])

  (close! [this test]
    (doseq [[_ w] @writers]
      (try
        (.shutdown w 5 TimeUnit/SECONDS)
        (catch Exception _)))
    (.close topic-client)
    (.close query-client)
    (.close transport)))

(defn new-client
  [opts]
  (Client. nil nil nil (atom {}) (atom false)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (append/test (assoc (select-keys opts [:key-count
                                              :min-txn-length
                                              :max-txn-length
                                              :max-writes-per-key])
                           :consistency-models [(:model opts)])))
      (assoc :client (new-client opts))))
