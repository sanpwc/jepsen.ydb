(ns jepsen.ydb.topic-table
  "Mixes table and topic operations inside single YDB transactions to catch
   atomicity violations between the SQL API and the Topic API (see issue
   ydb-platform/jepsen.ydb#30). Table-key micro-ops are unrestricted, exactly
   like jepsen.ydb.append. Topic-key micro-ops are restricted (see
   simplify-topic-mops for the exact rules): topic own-writes are invisible
   within the same transaction, and a topic replay read isn't pinned to the
   transaction's snapshot, so an unrestricted mix would surface
   false-positive read-your-own-writes / repeatable-read / torn-read
   anomalies that are inherent to topic semantics, not atomicity bugs (this
   is exactly what happened in the topics-only POC on branch topic-poc, and
   in a real cluster run of an earlier, insufficiently-restricted version of
   this workload). Writes are always unrestricted -- they only take effect
   atomically at commit. Restricting reads makes topic reads/writes behave,
   from the checker's point of view, like ordinary list-append operations --
   so the unmodified Elle list-append checker (via jepsen.tests.cycle.append)
   can be reused across the combined table+topic keyspace without a
   topic-specific checker."
  (:require [jepsen.client :as client]
            [jepsen.generator :as gen]
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
   restricts reads of topic keys much more heavily than table keys (see its
   docstring), so a given topic key's :append(s) are comparatively unlikely
   to ever be read back again by the ordinary random generator alone,
   without an explicit final sweep."
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
  "Given a transaction's micro-ops, returns a simplified vector safe to run
   against topics that aren't snapshot-isolated within a transaction. Two
   rules, applied in order:

   1. Drops a topic-key read if that same key was already appended to
      earlier in this transaction. Topic own-writes aren't visible within
      the same transaction, so a read immediately reflecting them would
      look like an internal-consistency violation to Elle (this is the
      original per-key read-your-own-writes/repeatable-read concern from
      the abandoned topics-only POC on branch topic-poc). A read that
      precedes the write to the same key is unaffected -- it isn't
      expected to see a write that, in program order, hasn't happened yet.

   2. After (1), if any topic-key read remains, keeps only the FIRST one
      and drops every OTHER read in the transaction -- table reads
      included. execute-topic-read! isn't attached to the transaction (a
      topic replay read can't be snapshot-pinned regardless -- see its
      docstring), so it reflects an independent point in real time. Two or
      more reads in one transaction, where at least one is a topic read,
      can therefore observe two different, mutually inconsistent moments
      -- e.g. a transaction reading topic key A and table key B could see
      A already reflecting some other transaction T's atomic write to both
      A and B, while B still reflects pre-T state (or vice versa), a torn
      view no valid serialization order could produce. Elle would
      correctly flag this as impossible, but the actual cause would be our
      own non-snapshot topic reads, not a real YDB bug. Restricting to at
      most one read per transaction whenever a topic key is involved
      removes anything for a second, differently-timed read to tear
      against. Writes are unrestricted throughout -- they only take
      effect atomically at commit, so they never observe a torn view the
      way a second read could.

   Applied at the GENERATOR level (see workload, via gen/map), not
   client-side in invoke! -- Elle's checker (elle.txn/intermediate-write-
   indices, used for G1b/intermediate-read detection, and potentially other
   analyses) reads straight from the raw history, including :invoke entries,
   which Jepsen always logs with the exact value the generator produced,
   before any client ever sees it. If we simplified only inside invoke! (as
   an earlier version of this code did), the :invoke entry would still show
   the original, never-executed extra touches, and Elle would treat those
   phantom writes/reads as real. Simplifying at the generator means the
   :invoke entry Jepsen logs already matches what actually runs, so there's
   nothing for the checker to misread."
  [test mops]
  (let [written (volatile! #{})
        rule-1 (vec (keep (fn [[f k _ :as mop]]
                             (cond
                               (= f :append)
                               (do (when (topic-key? test k) (vswap! written conj k))
                                   mop)

                               (and (= f :r) (topic-key? test k) (contains? @written k))
                               nil

                               :else mop))
                           mops))
        first-topic-read-index (->> rule-1
                                     (map-indexed vector)
                                     (some (fn [[i [f k _]]]
                                             (when (and (= f :r) (topic-key? test k)) i))))]
    (if (nil? first-topic-read-index)
      rule-1
      (vec (keep-indexed (fn [i [f _ _ :as mop]]
                            (cond
                              (= i first-topic-read-index) mop
                              (= f :append) mop
                              :else nil))
                          rule-1)))))

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
                    ; txn is already simplified by the generator (see
                    ; workload/simplify-topic-mops) -- nothing to do here but
                    ; execute it.
                    txn' (->> txn
                              (mapcat (partial apply-mop! test tx topic-client writers))
                              (into []))]
                (assoc op :type :ok, :value txn'))))))))

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

(defn new-simplify-topic-mops-in-op
  "Builds a single-arity fn (NOT via partial/clojure.core -- see below) that
   transforms a generated op by simplifying its :value -- see
   simplify-topic-mops. Passed to gen/map so the transaction Jepsen logs at
   :invoke time is already what will actually run.

   Must be a plain (fn [op] ...) closure, not (partial f opts): gen/map
   picks which arity to call f with (1 or 3 args) by reflecting on
   (.getDeclaredMethods (class f)) for the highest-arity `invoke` method it
   finds. clojure.core/partial's returned function genuinely implements
   invoke at several arities (0, 1, 2, 3, & more), all of which just forward
   to the wrapped function with the fixed args prepended -- so reflection
   sees a spurious 3-arg invoke and gen/map calls it as (f op test ctx),
   which prepends opts too and calls the real 2-arity fn with 4 args,
   throwing ArityException. A plain (fn [op] ...) only ever has a genuine
   1-arg invoke, so reflection reports arity 1 correctly."
  [opts]
  (fn [op]
    (if (= :txn (:f op))
      (update op :value (partial simplify-topic-mops opts))
      op)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (update (append/test (assoc (select-keys opts [:min-txn-length :max-txn-length :max-writes-per-key])
                                   :key-count (+ (:table-key-count opts) (:topic-key-count opts))
                                   :consistency-models [(:model opts)]))
               :generator (partial gen/map (new-simplify-topic-mops-in-op opts))))
      (assoc :client (new-client opts)
             :final-generator (new-final-reads-gen))))
