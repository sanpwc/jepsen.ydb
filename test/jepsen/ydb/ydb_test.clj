(ns jepsen.ydb.ydb-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb :as ydb]))

(deftest validate-opts-test
  (testing "Valid: serializable without opindex"
    (is (= {:model :serializable :with-opindex false}
           (ydb/validate-opts {:model :serializable :with-opindex false}))))

  (testing "Valid: serializable with opindex"
    (is (= {:model :serializable :with-opindex true}
           (ydb/validate-opts {:model :serializable :with-opindex true}))))

  (testing "Valid: snapshot-isolation without opindex"
    (is (= {:model :snapshot-isolation :with-opindex false}
           (ydb/validate-opts {:model :snapshot-isolation :with-opindex false}))))

  (testing "Valid: snapshot-isolation without opindex key"
    (is (= {:model :snapshot-isolation}
           (ydb/validate-opts {:model :snapshot-isolation}))))

  (testing "Invalid: snapshot-isolation with opindex"
    (is (thrown? IllegalArgumentException
                (ydb/validate-opts {:model :snapshot-isolation
                                    :with-opindex true}))))

  (testing "Valid: read-committed without opindex"
    (is (= {:model :read-committed :with-opindex false}
           (ydb/validate-opts {:model :read-committed :with-opindex false}))))

  (testing "Valid: read-committed without opindex key"
    (is (= {:model :read-committed}
           (ydb/validate-opts {:model :read-committed}))))

  (testing "Invalid: read-committed with opindex"
    (is (thrown? IllegalArgumentException
                (ydb/validate-opts {:model :read-committed
                                    :with-opindex true}))))

  (testing "Valid: empty opts"
    (is (= {} (ydb/validate-opts {}))))

  (testing "Valid: no model specified"
    (is (= {:with-opindex true}
           (ydb/validate-opts {:with-opindex true}))))

  (testing "Invalid: check error message"
    (is (thrown-with-msg? IllegalArgumentException
                          #"--with-opindex is not compatible with --model snapshot-isolation"
                          (ydb/validate-opts {:model :snapshot-isolation
                                              :with-opindex true})))))

