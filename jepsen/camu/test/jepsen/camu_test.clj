(ns jepsen.camu-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.camu :as camu]))

(deftest validate-opts-accepts-kafka-mixed
  (is (= {:api :kafka :workload :mixed}
         (camu/validate-opts! {:api :kafka :workload :mixed}))))

(deftest validate-opts-accepts-http-idempotent
  (is (= {:api :http :workload :idempotent}
         (camu/validate-opts! {:api :http :workload :idempotent}))))

(deftest validate-opts-accepts-kafka-idempotent
  (testing "Kafka + idempotent is valid — Camu handles InitProducerID and sequence dedup server-side"
    (is (= {:api :kafka :workload :idempotent}
           (camu/validate-opts! {:api :kafka :workload :idempotent})))))

(deftest validate-opts-rejects-kafka-replica-read-mode
  (testing "Kafka + replica read-mode is rejected (independent of workload)"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"only supports leader read-mode"
         (camu/validate-opts! {:api :kafka :workload :mixed :read-mode :replica})))))

(deftest leader-read-mode-checks-only-leader-visibility
  (is (true? (camu/leader-read-mode? {})))
  (is (true? (camu/leader-read-mode? {:read-mode :leader})))
  (is (false? (camu/leader-read-mode? {:read-mode :replica})))
  (is (false? (camu/leader-read-mode? {:read-mode :any}))))
