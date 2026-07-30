(ns jepsen.camu-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.camu :as camu]))

(deftest validate-opts-rejects-kafka-idempotent
  (testing "Kafka + idempotent workload should be rejected"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"does not support the idempotent workload"
         (camu/validate-opts! {:api :kafka :workload :idempotent})))))

(deftest validate-opts-rejects-kafka-idemponent-read-mode-replica
  (testing "Kafka + idempotent + replica read-mode — workload rejection takes precedence"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"does not support the idempotent workload"
         (camu/validate-opts! {:api :kafka :workload :idempotent :read-mode :replica})))))

(deftest validate-opts-accepts-kafka-mixed
  (is (= {:api :kafka :workload :mixed}
         (camu/validate-opts! {:api :kafka :workload :mixed}))))

(deftest validate-opts-accepts-http-idempotent
  (is (= {:api :http :workload :idempotent}
         (camu/validate-opts! {:api :http :workload :idempotent}))))

(deftest validate-opts-rejects-kafka-replica-read-mode
  (testing "Kafka + replica read-mode is rejected (independent of workload)"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"only supports leader read-mode"
         (camu/validate-opts! {:api :kafka :workload :mixed :read-mode :replica})))))