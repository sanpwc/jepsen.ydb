(ns jepsen.ydb.topic-table-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb.topic-table :as topic-table]))

(def test-map {:table-key-count 3 :topic-key-count 2})

(deftest topic-key?-test
  (testing "First topic-key-count keys of each block of total-key-count are topic keys"
    (is (true? (topic-table/topic-key? test-map 0)))
    (is (true? (topic-table/topic-key? test-map 1)))
    (is (false? (topic-table/topic-key? test-map 2)))
    (is (false? (topic-table/topic-key? test-map 3)))
    (is (false? (topic-table/topic-key? test-map 4))))
  (testing "Classification is stable across key rotation (fresh-key grows the key space unboundedly)"
    ; total-key-count is 5 here, so keys repeat the same table/topic pattern
    ; every 5 -- a rotated key far past the initial window must classify the
    ; same way as its congruent key within the first window.
    (is (= (topic-table/topic-key? test-map 0) (topic-table/topic-key? test-map 100000)))
    (is (= (topic-table/topic-key? test-map 2) (topic-table/topic-key? test-map 100002)))
    (is (true? (topic-table/topic-key? test-map 100001)))
    (is (false? (topic-table/topic-key? test-map 100004)))))

(deftest simplify-topic-mops-test
  (testing "Keeps only the first touch of a repeated topic key"
    (is (= [[:append 0 1] [:r 2 nil]]
           (topic-table/simplify-topic-mops test-map [[:append 0 1] [:r 0 nil] [:r 2 nil]]))))
  (testing "A read before a later append of the same topic key also collapses to the read"
    (is (= [[:r 1 nil]]
           (topic-table/simplify-topic-mops test-map [[:r 1 nil] [:append 1 5]]))))
  (testing "Table keys are never touched, however many times they repeat"
    (is (= [[:append 2 1] [:r 2 nil] [:append 2 2] [:r 3 nil]]
           (topic-table/simplify-topic-mops test-map [[:append 2 1] [:r 2 nil] [:append 2 2] [:r 3 nil]]))))
  (testing "A mix of table and topic keys preserves relative order"
    (is (= [[:append 2 1] [:append 0 9] [:r 3 nil]]
           (topic-table/simplify-topic-mops test-map [[:append 2 1] [:append 0 9] [:append 0 9] [:r 3 nil]])))))
