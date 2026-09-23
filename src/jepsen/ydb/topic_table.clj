(ns jepsen.ydb.topic-table
  "Mixes table and topic operations inside single YDB transactions to catch
   atomicity violations between the SQL API and the Topic API (see issue
   ydb-platform/jepsen.ydb#30). Table-key micro-ops are unrestricted, exactly
   like jepsen.ydb.append. Topic-key micro-ops are restricted to at most one
   touch per key per transaction (see simplify-topic-mops): topic own-writes
   are invisible within the same transaction, and replay reads aren't
   snapshot-isolated, so an unrestricted mix would surface false-positive
   read-your-own-writes / repeatable-read anomalies that are inherent to
   topic semantics, not atomicity bugs (this is exactly what happened in the
   topics-only POC on branch topic-poc). Restricting to a single touch per
   topic key per transaction makes topic reads/writes behave, from the
   checker's point of view, like ordinary list-append operations -- so the
   unmodified Elle list-append checker (via jepsen.tests.cycle.append) can be
   reused across the combined table+topic keyspace without a topic-specific
   checker."
  (:require [jepsen.client :as client]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ydb.append :as table]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable]
            [jepsen.ydb.topic :as topic])
  (:import (java.util.concurrent TimeUnit)))

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(def touched-topic-keys
  "Every topic key any transaction has ever attempted an :r or :append on,
   across the whole test run. Used to build a final read sweep (see
   final-reads-gen) -- jepsen 0.3.10 (pinned in project.clj) predates
   jepsen.tests.cycle.core/final-gen and max-key-tracker (added in a later
   Jepsen release), so jepsen.tests.cycle.append/test provides no built-in
   guarantee that every key gets read again before the test ends. That
   matters more here than for table-only workloads: simplify-topic-mops
   deliberately touches each topic key at most once per transaction, so
   without an explicit final sweep, a topic key's only :append could go
   completely unobserved by any read for the rest of the test."
  (atom #{}))

(defn new-final-reads-gen
  "Builds a fresh generator (an arity-0 function; see jepsen.generator's
   Fn/AFunction handling of plain functions) that reads every topic key ever
   touched, once, in batches. Evaluated lazily -- only invoked once the main
   phase has finished and touched-topic-keys is fully populated, via
   ydb-test's existing (:final-generator workload) handling (already wired
   up for kafka-topic; --kafka-final-time-limit governs its time budget for
   any workload, including this one).

   The returned function is one-shot (guarded by emitted?): jepsen's Fn
   generator wrapper calls an arity-0 generator function again once the
   sequence it returned is exhausted, falling back to treating a fresh call
   as a fresh generator -- without the guard, this would call us forever,
   endlessly re-reading the same keys until the time limit cuts it off,
   rather than reading each key once and finishing."
  []
  (let [emitted? (atom false)]
    (fn []
      (when (compare-and-set! emitted? false true)
        (->> @touched-topic-keys
             sort
             (partition-all 8)
             (map (fn [batch] {:type :invoke, :f :txn, :value (mapv (fn [k] [:r k nil]) batch)})))))))

(defn total-key-count
  "The combined table+topic key space size, used both to configure the
   underlying append/test generator's :key-count and to classify keys here.
   Kept as a single function so the two can never drift apart."
  [test]
  (+ (:table-key-count test) (:topic-key-count test)))

(defn topic-key?
  "Classifies a (potentially unbounded, ever-rotating) Jepsen key as
   table or topic, deterministically by its value mod the total key count --
   not by an initial [0, topic-key-count) range -- since elle.txn/fresh-key
   retires and replaces keys as the test runs, so the key space grows well
   past the initial window over a test's lifetime."
  [test k]
  (< (mod (long k) (total-key-count test)) (:topic-key-count test)))

(defn simplify-topic-mops
  "Given a transaction's micro-ops, drops every micro-op after the first one
   touching a given topic key (whether that first one is a :r or an
   :append). Table-key micro-ops are always kept unchanged, in any number."
  [test mops]
  (let [seen (volatile! #{})]
    (vec (filter (fn [[_ k _]]
                   (or (not (topic-key? test k))
                       (if (contains? @seen k)
                         false
                         (do (vswap! seen conj k) true))))
                 mops))))

(defn apply-mop!
  [test tx topic-client writers [f k v :as mop]]
  (if (topic-key? test k)
    (do
      (swap! touched-topic-keys conj k)
      (case f
        :r [[f k (topic/execute-topic-read! test topic-client k)]]
        :append [(do (topic/execute-topic-append! test tx topic-client writers k v) mop)]))
    (case f
      :r [[f k (table/execute-list-read test tx k)]]
      :append [(do (table/execute-list-append test tx nil k v) mop)])))

(defrecord Client [transport query-client topic-client writers ballast setup?]
  client/Client
  (open! [this test node]
    (let [transport (conn/open-transport test node)
          query-client (conn/open-query-client transport)
          topic-client (conn/open-topic-client transport)]
      (assoc this :transport transport :query-client query-client :topic-client topic-client)))

  (setup! [this test]
    (once-per-cluster
     setup?
     (reset! touched-topic-keys #{})
     (table/drop-initial-tables test query-client)
     (table/create-initial-tables test query-client)
     (topic/drop-initial-topic test query-client)
     (topic/create-initial-topic test query-client)))

  (invoke! [_ test op]
    (table/with-ballast ballast
      (debug-info/with-debug-info
        (conn/with-errors op
          (conn/with-session [session query-client]
            (conn/with-transaction [tx [session (:model test)]]
              (let [txn (:value op)
                    txn' (simplify-topic-mops test txn)
                    op' (if (not= txn txn') (assoc op :modified-txn txn') op)
                    txn'' (->> txn'
                               (mapcat (partial apply-mop! test tx topic-client writers))
                               (into []))]
                (assoc op' :type :ok, :value txn''))))))))

  (teardown! [this test])

  (close! [this test]
    (doseq [[_ w] @writers]
      (try
        (-> w .shutdown (.get 5 TimeUnit/SECONDS))
        (catch Exception _)))
    (.close topic-client)
    (.close query-client)
    (.close transport)))

(defn new-client
  [opts]
  (Client. nil nil nil (atom {}) (table/new-ballast (:ballast-size opts)) (atom false)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (append/test (assoc (select-keys opts [:min-txn-length :max-txn-length :max-writes-per-key])
                           :key-count (+ (:table-key-count opts) (:topic-key-count opts))
                           :consistency-models [(:model opts)])))
      (assoc :client (new-client opts)
             :final-generator (new-final-reads-gen))))
