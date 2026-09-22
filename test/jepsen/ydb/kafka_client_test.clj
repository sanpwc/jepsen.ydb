(ns jepsen.ydb.kafka-client-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb.kafka-client :as kc])
  (:import (java.util Properties)))

(deftest properties
  (testing "Values are stringified"
    (let [^Properties p (kc/->properties {"a" 1 "b" true})]
      (is (= "1" (.getProperty p "a")))
      (is (= "true" (.getProperty p "b"))))))

(deftest plain-username
  (testing "db-name is appended when the username has none"
    (is (= "u@/local" (kc/plain-username {:kafka-username "u" :db-name "/local"}))))
  (testing "Defaults to a synthetic username"
    (is (= "jepsen@/local" (kc/plain-username {:db-name "/local"}))))
  (testing "An explicit @ is left alone"
    (is (= "u@/other" (kc/plain-username {:kafka-username "u@/other" :db-name "/local"})))))

(deftest auth-config
  (testing "Disabled explicitly"
    (is (= {} (kc/auth-config {:kafka-sasl? false}))))
  (testing "PLAIN appends db-name to the username, defaulting the password"
    (is (= {"security.protocol" "SASL_PLAINTEXT"
            "sasl.mechanism"    "PLAIN"
            "sasl.jaas.config"
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u@/local\" password=\"p\";"}
           (kc/auth-config {:kafka-sasl? true
                            :kafka-sasl-mechanism "PLAIN"
                            :kafka-username "u"
                            :kafka-password "p"
                            :db-name "/local"}))))
  (testing "SCRAM leaves the username as-is (database is resolved from the endpoint)"
    (is (= "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";"
           (get (kc/auth-config {:kafka-sasl? true
                                 :kafka-sasl-mechanism "SCRAM-SHA-256"
                                 :kafka-username "u"
                                 :kafka-password "p"
                                 :db-name "/local"})
                "sasl.jaas.config")))))

(deftest producer-config
  (let [test {:kafka-port 9092 :kafka-transaction-timeout-ms 10000}]
    (testing "Compression is always disabled"
      (is (= "none" (get (kc/producer-config test "n1" nil) "compression.type"))))
    (testing "Bootstrap servers use the node and kafka port"
      (is (= "n1:9092" (get (kc/producer-config test "n1" nil) "bootstrap.servers"))))
    (testing "Transactional producers are idempotent and have an id"
      (let [config (kc/producer-config test "n1" "tx-1")]
        (is (= "tx-1" (get config "transactional.id")))
        (is (= true (get config "enable.idempotence")))
        (is (= 10000 (get config "transaction.timeout.ms")))))
    (testing "Non-transactional producers have no transactional id"
      (is (not (contains? (kc/producer-config test "n1" nil) "transactional.id"))))))

(deftest consumer-config
  (let [config (kc/consumer-config {:kafka-port 9092
                                    :kafka-isolation-level "read_committed"}
                                   "n1")]
    (testing "A group id is always set"
      (is (= kc/consumer-group (get config "group.id"))))
    (testing "Isolation level comes from the test"
      (is (= "read_committed" (get config "isolation.level"))))
    (testing "Auto commit is off"
      (is (= false (get config "enable.auto.commit"))))))
