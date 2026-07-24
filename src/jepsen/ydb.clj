(ns jepsen.ydb
  (:gen-class)
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cli :as cli]
            [jepsen.control :as c]
            [jepsen.control.net :as control-net]
            [jepsen.db :as db]
            [jepsen.os :as os]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.net :as jepsen-net]
            [jepsen.net.proto :as net-proto]
            [jepsen.tests :as tests]
            [jepsen.control.util :as cu]
            [jepsen.ydb.cli.clean :refer [clean-valid-cmd]]
            [jepsen.ydb.append :as append]
            [jepsen.ydb.append-with-deletes :as append-with-deletes]))

;;; ─── Constants ───────────────────────────────────────────────────────────────

(def dynamic-service "kikimr-multi@31003.service")
(def storage-service "kikimr.service")

;;; ─── Dual-stack network (IPv4 + IPv6) ───────────────────────────────────────

(defn ipv6?
  "Returns true when ip-str is an IPv6 address (contains a colon)."
  [ip-str]
  (str/includes? ip-str ":"))

(def dual-stack-net
  "Drop-in replacement for jepsen.net/iptables that transparently handles both
  IPv4 (iptables) and IPv6 (ip6tables) node addresses."
  (reify net-proto/Net

    (drop! [net test src dest]
      (c/on-nodes test [dest]
        (fn [_ _]
          (let [ip  (control-net/ip src)
                cmd (if (ipv6? ip) :ip6tables :iptables)]
            (c/su (c/exec cmd :-A :INPUT :-s ip :-j :DROP :-w))))))

    ;; FIX #1: wrap each exec in try/catch.
    ;; A non-zero exit (ip6tables not installed, chains already empty) causes
    ;; jepsen's SSH retry layer to return a PersistentHashMap instead of
    ;; throwing, which blows up with ClassCastException in retry.clj.
    (heal! [net test]
      (c/with-test-nodes test
        (c/su
          (try
            (c/exec :iptables  :-F :-w)
            (catch Exception e
              (warn "iptables -F -w failed during heal!:" (.getMessage e))))
          (try
            (c/exec :ip6tables :-F :-w)
            (catch Exception e
              (warn "ip6tables -F -w failed during heal!:" (.getMessage e)))))))

    (slow! [net test]
      (c/with-test-nodes test
        (c/su (c/exec :tc :qdisc :add :dev (jepsen-net/net-dev)
                      :root :netem :delay :50ms :10ms
                      :distribution :normal))))

    (slow! [net test {:keys [mean variance distribution]
                      :or   {mean 50 variance 10 distribution :normal}}]
      (c/with-test-nodes test
        (c/su (c/exec :tc :qdisc :add :dev (jepsen-net/net-dev)
                      :root :netem :delay
                      (str mean "ms") (str variance "ms")
                      :distribution distribution))))

    (flaky! [net test]
      (c/with-test-nodes test
        (c/su (c/exec :tc :qdisc :add :dev (jepsen-net/net-dev)
                      :root :netem :loss "20%" "75%"))))

    (fast! [net test]
      (c/with-test-nodes test
        (try
          (c/su (c/exec :tc :qdisc :del :dev (jepsen-net/net-dev) :root))
          (catch RuntimeException e
            (when-not (re-find #"Error: Cannot delete qdisc with handle of zero\."
                               (.getMessage e))
              (throw e))))))

    (shape! [net test nodes behavior]
      (jepsen-net/shape! jepsen-net/iptables test nodes behavior))

    net-proto/PartitionAll
    (drop-all! [net test grudge]
      (c/on-nodes test (keys grudge)
        (fn [_ node]
          (let [srcs  (get grudge node)
                ips   (map control-net/ip srcs)
                ipv4s (remove ipv6? ips)
                ipv6s (filter ipv6? ips)]
            (c/su
              (when (seq ipv4s)
                (c/exec :iptables  :-A :INPUT :-s (str/join "," ipv4s) :-j :DROP :-w))
              (when (seq ipv6s)
                (c/exec :ip6tables :-A :INPUT :-s (str/join "," ipv6s) :-j :DROP :-w)))))))))

;;; ─── Systemd helpers ─────────────────────────────────────────────────────────

(defn sigkill-and-wait!
  "Sends SIGKILL to a systemd unit and blocks until it is inactive."
  [unit]
  (c/exec :bash :-c (str "systemctl kill -s SIGKILL " unit "; "
                          "while systemctl is-active --quiet " unit "; do "
                          "  sleep 0.2; "
                          "done")))

(defn safe-start!
  "Starts a systemd unit; logs a warning instead of crashing if it fails."
  [unit]
  (try
    (c/exec :systemctl :start unit)
    (catch Exception e
      (warn "Failed to start" unit ":" (.getMessage e)))))

;; FIX #2: safe-pause! mirrors safe-resume!.
;; systemctl kill -s SIGSTOP exits non-zero when the unit is already dead,
;; triggering the same PersistentHashMap ClassCastException in retry.clj.
(defn safe-pause!
  "Sends SIGSTOP to a systemd unit; logs a warning if the unit is not found."
  [unit]
  (try
    (c/exec :systemctl :kill :-s :SIGSTOP unit)
    (catch Exception e
      (warn "Failed to pause" unit ":" (.getMessage e)))))

(defn safe-resume!
  "Sends SIGCONT to a systemd unit; logs a warning if the unit is not found."
  [unit]
  (try
    (c/exec :systemctl :kill :-s :SIGCONT unit)
    (catch Exception e
      (warn "Failed to resume" unit ":" (.getMessage e)))))

;; FIX #3: safe-restart! for --nemesis all.
;; When pause + restart faults run concurrently, systemctl restart on a
;; SIGSTOP'd unit can time out and return non-zero.
(defn safe-restart!
  "Restarts a systemd unit; logs a warning instead of crashing if it fails."
  [unit]
  (try
    (c/exec :systemctl :restart unit)
    (catch Exception e
      (warn "Failed to restart" unit ":" (.getMessage e)))))

;;; ─── Database (with Pause support for nc/pause-package) ─────────────────────

(defn make-db []
  (reify db/DB
    (setup! [_ _test node]
      (info "YDB connection pre-check on node:" node))
    (teardown! [_ _test node]
      (info "YDB testing finished on node:" node))

    db/Pause
    ;; FIX #2 applied: use safe-pause! so a dead unit does not crash the nemesis.
    (pause! [_ _test node]
      (info "SIGSTOP dynamic+storage on" node)
      (c/su
        (safe-pause! dynamic-service)
        (safe-pause! storage-service))
      :paused)

    (resume! [_ _test node]
      (info "SIGCONT dynamic+storage on" node)
      (c/su
        (safe-resume! dynamic-service)
        (safe-resume! storage-service))
      :resumed)))

;;; ─── Custom service nemesis (kill-dynamic/storage, restart-dynamic/storage) ──

(def service-faults
  "All faults handled by the service nemesis."
  #{:kill-dynamic :kill-storage :restart-dynamic :restart-storage})

;; All :f values this nemesis can receive, including recovery ops.
(def service-nemesis-fs
  #{:kill-dynamic :kill-storage
    :restart-dynamic :restart-storage
    :start-dynamic :start-storage})

(defn service-nemesis
  "A nemesis that independently kills, restarts, or starts YDB service units."
  []
  (reify
    nemesis/Nemesis
    (setup! [this _test] this)

    (invoke! [_this test op]
      (let [target-nodes (or (:value op) [(rand-nth (:nodes test))])]
        (c/on-nodes test target-nodes
          (fn [_ node]
            (c/su
              (case (:f op)
                :kill-dynamic
                (do (info "SIGKILL dynamic on" node)
                    (sigkill-and-wait! dynamic-service))

                :kill-storage
                (do (info "SIGKILL storage on" node)
                    (sigkill-and-wait! storage-service))

                ;; FIX #3 applied: safe-restart! so a SIGSTOP'd unit doesn't crash.
                :restart-dynamic
                (do (info "Restarting dynamic on" node)
                    (safe-restart! dynamic-service))

                :restart-storage
                (do (info "Restarting storage on" node)
                    (safe-restart! storage-service))

                :start-dynamic
                (do (info "Starting dynamic on" node)
                    (safe-start! dynamic-service))

                :start-storage
                (do (info "Starting storage on" node)
                    (safe-start! storage-service))))))
        (assoc op :value target-nodes)))

    (teardown! [_this _test])

    ;; FIX #4: implement nemesis/Reflection so nc/compose-packages can inspect
    ;; which :f values this nemesis handles. Without this, compose-packages
    ;; throws IllegalArgumentException: No implementation of method :fs.
    nemesis/Reflection
    (fs [_this] service-nemesis-fs)))

(defn- kill-cycle-gen
  "Infinite lazy sequence: kill-op → sleep → start-op → repeat."
  [kill-f start-f interval]
  (lazy-cat
    [{:type :info :f kill-f}
     (gen/sleep interval)
     {:type :info :f start-f}]
    (kill-cycle-gen kill-f start-f interval)))

(defn- restart-cycle-gen
  "Infinite lazy sequence: restart-op → sleep → repeat."
  [restart-f interval]
  (lazy-cat
    [{:type :info :f restart-f}
     (gen/sleep interval)]
    (restart-cycle-gen restart-f interval)))

(defn service-package
  [{:keys [faults interval nodes] :or {interval 5}}]
  (let [active (filter service-faults faults)]
    (when (seq active)
      (let [per-fault-gens
            (for [f active]
              (case f
                :kill-dynamic    (kill-cycle-gen    :kill-dynamic    :start-dynamic    interval)
                :kill-storage    (kill-cycle-gen    :kill-storage    :start-storage    interval)
                :restart-dynamic (restart-cycle-gen :restart-dynamic interval)
                :restart-storage (restart-cycle-gen :restart-storage interval)))

            combined-gen (gen/mix per-fault-gens)]

        {:nemesis  (service-nemesis)
         :generator combined-gen
         ;; After gen/time-limit fires, ensure all services are up on all nodes.
         :final-generator
         (gen/phases
           {:type :info :f :start-dynamic :value nodes}
           {:type :info :f :start-storage :value nodes})
         :perf
         #{{:name "kill-dynamic"    :fs #{:kill-dynamic}    :color "#E74C3C"}
           {:name "start-dynamic"   :fs #{:start-dynamic}   :color "#2ECC71"}
           {:name "kill-storage"    :fs #{:kill-storage}    :color "#C0392B"}
           {:name "start-storage"   :fs #{:start-storage}   :color "#27AE60"}
           {:name "restart-dynamic" :fs #{:restart-dynamic} :color "#F39C12"}
           {:name "restart-storage" :fs #{:restart-storage} :color "#E67E22"}}}))))


;;; ─── Workload helpers ────────────────────────────────────────────────────────

(defn ydb-workload [opts]
  (case (:workload-name opts)
    "append"              (append/workload opts)
    "append-with-deletes" (append-with-deletes/workload opts)))

(defn ydb-unhandled-exceptions [opts]
  (let [wrapped (checker/unhandled-exceptions)]
    (if (:allow-exceptions opts)
      wrapped
      (reify checker/Checker
        (check [_this test history opts]
          (let [result (checker/check wrapped test history opts)]
            (if (and (:valid? result) (seq (:exceptions result)))
              (merge result {:valid? false})
              result)))))))

(defn validate-opts [opts]
  (when (and (= (:model opts) :snapshot-isolation)
             (:with-opindex opts))
    (throw (IllegalArgumentException.
            "--with-opindex is not compatible with --model snapshot-isolation")))
  opts)

;;; ─── Test builder ────────────────────────────────────────────────────────────

(defn ydb-test [opts]
  (validate-opts opts)
  (let [workload (ydb-workload opts)
        the-db   (make-db)

        ;; Standard packages for partition, clock, pause.
        nc-faults (filter #{:partition :clock :pause} (:nemesis opts))
        nc-pkgs   (nc/nemesis-packages
                    {:db       the-db
                     :nodes    (:nodes opts)
                     :faults   nc-faults
                     :partition {:targets [:one]}
                     :pause     {:targets [:one]}
                     :interval (:nemesis-interval opts)})

        ;; Custom service package for kill/restart faults.
        svc-pkg   (service-package
                    {:faults   (:nemesis opts)
                     :interval (:nemesis-interval opts)
                     :nodes    (:nodes opts)})

        all-pkgs  (remove nil?
                    (concat
                      (filter (fn [p] (some? (:generator p))) nc-pkgs)
                      [svc-pkg]))

        ;; FIX #5: when no faults are selected (--nemesis absent or none),
        ;; all-pkgs is empty. compose-packages on empty seq crashes.
        ;; Fall back to a trivial noop structure.
        nemesis   (if (seq all-pkgs)
                    (nc/compose-packages all-pkgs)
                    {:nemesis         nemesis/noop
                     :generator       nil
                     :final-generator nil
                     :perf            #{}})

        nem-gen   (:generator nemesis)
        final-gen (:final-generator nemesis)]

    (merge tests/noop-test
           opts
           {:name               "ydb"
            :db                 the-db
            :os                 ubuntu/os
            :net                dual-stack-net
            :concurrency-factor 1
            :ssh                {:dummy false :strict-host-key-checking false}
            :client             (:client workload)
            :nemesis            (:nemesis nemesis)
            :checker            (checker/compose
                                  {:perf       (checker/perf {:nemeses (:perf nemesis)})
                                   :clock      (checker/clock-plot)
                                   :stats      (checker/stats)
                                   :exceptions (ydb-unhandled-exceptions opts)
                                   :workload   (:checker workload)})
            ;; FIX #5 cont.: gen/nemesis MUST always wrap the client generator
            ;; even when nem-gen is nil. Without gen/nemesis the nemesis worker
            ;; process receives client ops (:f :txn, :type :invoke) and crashes
            ;; with "type should be :info". gen/nemesis nil means "no nemesis
            ;; ops" but still correctly routes client ops to client workers only.
            :generator
            (gen/phases
              (->> (:generator workload)
                   (gen/stagger (/ (:rate opts)))
                   (gen/nemesis nem-gen)
                   (gen/time-limit (:time-limit opts)))
              (gen/log "Recovering cluster state after test...")
              (when final-gen
                (gen/nemesis final-gen)))})))

;;; ─── CLI ─────────────────────────────────────────────────────────────────────

(def all-nemesis-faults
  "Full set of accepted nemesis fault keywords."
  #{:pause :partition :clock
    :kill-dynamic :kill-storage
    :restart-dynamic :restart-storage})

(def special-nemeses
  "Named shorthand groups."
  {:none []
   :all  (vec all-nemesis-faults)})

(defn parse-nemesis-spec
  "Parses a comma-separated nemesis string into a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (map (comp keyword str/trim))
       (mapcat #(get special-nemeses % [%]))))

(defn valid-probability? [v] (and (>= v 0.0) (<= v 1.0)))
(defn valid-read-replicas? [v] (>= v 0))

(def cli-opts
  [[nil "--db-name DBNAME"               "YDB database name."
    :default "/local"]
   [nil "--db-port NUM"                  "YDB database port."
    :default 2135 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--db-table NAME"                "YDB table name."
    :default "jepsen_test"]
   [nil "--workload-name NAME"           "YDB workload name."
    :default "append"]
   [nil "--model MODEL"                  "Consistency model to check."
    :default :ydb-serializable :parse-fn keyword]
   [nil "--allow-exceptions"             "Allow unhandled exceptions."
    :default false]
   [nil "--partition-size-mb NUM"        "Table partition size in MBs."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--initial-partition-count NUM"  "Initial number of partitions."
    :default 30 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--initial-partition-keys NUM"   "Initial number of keys per partition."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--with-read-replicas NUM"       "Per-az read replicas."
    :default 0 :parse-fn parse-long :validate [valid-read-replicas? "Must be 0 or greater"]]
   [nil "--with-opindex"                 "Use additional opindex column."
    :default false]
   [nil "--with-changefeed"              "Use updates changefeed."
    :default false]
   [nil "--batch-single-ops"             "Execute single ops via batch query."
    :default false]
   [nil "--batch-ops-probability NUM"
    :default 0.0 :parse-fn parse-double :validate [valid-probability? "Must be 0.0–1.0"]]
   [nil "--batch-commit-probability NUM"
    :default 1.0 :parse-fn parse-double :validate [valid-probability? "Must be 0.0–1.0"]]
   [nil "--key-count NUM"                "Keys in active rotation."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--ballast-size NUM"             "Ballast bytes added to values."
    :default 1000 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--max-txn-length NUM"           "Max ops per transaction."
    :default 4 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--max-writes-per-key NUM"       "Max writes to any key."
    :default 16 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   ["-r" "--rate HZ"                     "Approximate request rate in hz."
    :default 100 :parse-fn read-string :validate [pos? "Must be positive"]]

   [nil "--nemesis FAULTS"
    (str "Comma-separated nemesis faults. Valid values: "
         "partition, clock, pause, "
         "kill-dynamic, kill-storage, restart-dynamic, restart-storage, "
         "all, none. Omit entirely to run without any nemesis.")
    :default []
    :parse-fn parse-nemesis-spec
    :validate [(partial every? all-nemesis-faults)
               (str "Each fault must be one of: " (str/join ", " (map name all-nemesis-faults))
                    ", all, none.")]]

   [nil "--nemesis-interval SECS"        "Seconds between nemesis operations."
    :default 5 :parse-fn read-string :validate [pos? "Must be positive"]]
   [nil "--store-type TYPE"              "Store type: 'row' or 'column'."
    :default "row"]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  ydb-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd)
                   (clean-valid-cmd))
            args))

