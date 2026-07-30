(ns jepsen.camu.checker-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.checker :as checker]
            [jepsen.camu.checker :as camu-checker]))

;;; read-your-writes-checker tests

(defn- ryw-check
  [history]
  (checker/check (camu-checker/read-your-writes-checker) {} history {}))

(defn- ok-produce-op
  [proc key partition & [offset]]
  {:type :ok :f :produce :process proc :time 0
   :value {:key key :partition partition :offset (or offset 0)}})

(defn- ok-consume-op
  [proc partition messages]
  {:type :ok :f :consume :process proc :time 0
   :value {:partition partition :messages messages}})

(defn- msg
  [key offset]
  {:key key :offset offset})

(deftest ryw-accepts-visible-write
  (let [checked (ryw-check
                 [(ok-produce-op 0 "k1" 0)
                  (ok-consume-op 0 0 [(msg "k1" 0)])])]
    (is (:valid? checked))
    (is (= 1 (:acked checked)))
    (is (zero? (:missing checked)))))

(deftest ryw-rejects-write-never-consumed
  (let [checked (ryw-check
                 [(ok-produce-op 0 "k1" 0)
                  (ok-consume-op 0 0 [])])]
    (is (false? (:valid? checked)))
    (is (= 1 (:missing checked)))
    (is (= "k1" (-> checked :violations first :key)))))

(deftest ryw-rejects-write-consumed-from-wrong-partition
  (let [checked (ryw-check
                 [(ok-produce-op 0 "k1" 0)
                  (ok-consume-op 0 1 [(msg "k1" 0)])])]
    (is (false? (:valid? checked)))
    (is (= 1 (:missing checked)))
    (is (= 0 (-> checked :violations first :partition)))))

(deftest ryw-counts-cross-process-visibility
  (let [checked (ryw-check
                 [(ok-produce-op 0 "k1" 0)
                  (ok-consume-op 1 0 [(msg "k1" 0)])])]
    (is (:valid? checked))
    (is (zero? (:missing checked)))))

(deftest ryw-ignores-info-produces
  (let [checked (ryw-check
                 [{:type :info :f :produce :process 0 :time 0
                   :value {:key "k1" :partition 0 :offset 0}}
                  (ok-consume-op 0 0 [])])]
    (is (:valid? checked))
    (is (zero? (:acked checked)))))

(deftest ryw-ignores-failed-consumes
  (let [checked (ryw-check
                 [(ok-produce-op 0 "k1" 0)
                  {:type :fail :f :consume :process 0 :time 0
                   :value {:partition 0 :messages [(msg "k1" 0)]}}])]
    (is (false? (:valid? checked)))
    (is (= 1 (:missing checked)))))

(deftest ryw-accepts-multiple-acked-writes-all-consumed
  (let [checked (ryw-check
                 [(ok-produce-op 0 "k1" 0 0)
                  (ok-produce-op 0 "k2" 0 1)
                  (ok-produce-op 1 "k3" 1 0)
                  (ok-consume-op 0 0 [(msg "k1" 0) (msg "k2" 1)])
                  (ok-consume-op 1 1 [(msg "k3" 0)])])]
    (is (:valid? checked))
    (is (= 3 (:acked checked)))
    (is (zero? (:missing checked)))))

(deftest ryw-reports-partial-missing
  (let [checked (ryw-check
                 [(ok-produce-op 0 "k1" 0 0)
                  (ok-produce-op 0 "k2" 0 1)
                  (ok-consume-op 0 0 [(msg "k1" 0)])])]
    (is (false? (:valid? checked)))
    (is (= 1 (:missing checked)))
    (is (= "k2" (-> checked :violations first :key)))))

;;; no-split-brain-checker tests
;;; These verify the checker works with drain data containing no :leader-epoch
;;; or :node — i.e. the Kafka protocol path where RecordMetadata doesn't
;;; expose leader epoch.

(defn- nsb-check
  [history]
  (checker/check (camu-checker/no-split-brain-checker) {} history {}))

(defn- ok-drain-op
  [partition messages]
  {:type :ok :f :drain :value {:partition partition :messages messages}})

(defn- drain-msg
  "A drain message as the Kafka client returns it: no :node, no :leader-epoch."
  [key value offset partition]
  {:key key :value value :offset offset :partition partition})

(deftest nsb-accepts-consistent-drain
  (let [checked (nsb-check
                 [(ok-drain-op 0 [(drain-msg "k1" "v1" 0 0)
                                  (drain-msg "k2" "v2" 1 0)])])]
    (is (:valid? checked))
    (is (nil? (:conflicts checked)))))

(deftest nsb-rejects-conflicting-values-at-same-offset
  (let [checked (nsb-check
                 [(ok-drain-op 0 [(drain-msg "k1" "v1" 0 0)])
                  (ok-drain-op 0 [(drain-msg "k2" "v2" 0 0)])])]
    (is (false? (:valid? checked)))
    (is (= 1 (count (:conflicts checked))))
    (is (= 0 (-> checked :conflicts first :partition)))
    (is (= 0 (-> checked :conflicts first :offset)))
    (is (= ["v1" "v2"] (-> checked :conflicts first :values)))))

(deftest nsb-accepts-same-value-at-same-offset
  (let [checked (nsb-check
                 [(ok-drain-op 0 [(drain-msg "k1" "v1" 0 0)])
                  (ok-drain-op 0 [(drain-msg "k1" "v1" 0 0)])])]
    (is (:valid? checked))))

(deftest nsb-works-across-multiple-partitions
  (let [checked (nsb-check
                 [(ok-drain-op 0 [(drain-msg "k1" "v1" 0 0)
                                  (drain-msg "k2" "v2" 1 0)])
                  (ok-drain-op 1 [(drain-msg "k3" "v3" 0 1)
                                  (drain-msg "k4" "v4" 1 1)])
                  (ok-drain-op 1 [(drain-msg "k5" "v5" 0 1)])])]
    (is (false? (:valid? checked)))
    (is (= 1 (count (:conflicts checked))))
    (is (= 1 (-> checked :conflicts first :partition)))))

;;; hw-global-monotonicity-checker tests

(defn- hwg-check
  [history]
  (checker/check (camu-checker/hw-global-monotonicity-checker) {} history {}))

(defn- ok-consume-hw
  [proc partition hw & [node]]
  {:type :ok :f :consume :process proc :time 0
   :value {:partition partition :high-watermark hw :node (or node "n1")}})

(deftest hwg-accepts-increasing-hw
  (let [checked (hwg-check
                 [(ok-consume-hw 0 0 10)
                  (ok-consume-hw 0 0 20)
                  (ok-consume-hw 0 0 30)])]
    (is (:valid? checked))
    (is (nil? (:violations checked)))))

(deftest hwg-accepts-equal-hw
  (let [checked (hwg-check
                 [(ok-consume-hw 0 0 50)
                  (ok-consume-hw 0 0 50)
                  (ok-consume-hw 0 0 50)])]
    (is (:valid? checked))))

(deftest hwg-detects-cross-node-hw-regression
  (let [checked (hwg-check
                 [(ok-consume-hw 0 0 100 "n1")
                  (ok-consume-hw 0 0 50 "n2")])]
    (is (false? (:valid? checked)))
    (is (= 1 (count (:violations checked))))))

(deftest hwg-per-node-checker-would-miss-this
  "Same scenario but the per-node checker would not catch this because
   each node's HW is independently monotonic. The global checker catches it."
  (let [checked (hwg-check
                 [(ok-consume-hw 0 0 10 "n1")
                  (ok-consume-hw 0 0 20 "n1")
                  (ok-consume-hw 0 0 5 "n2")
                  (ok-consume-hw 0 0 15 "n2")])]
    (is (false? (:valid? checked)))
    (is (= 1 (count (:violations checked))))))

(deftest hwg-multiple-partitions-independent
  (let [checked (hwg-check
                 [(ok-consume-hw 0 0 10)
                  (ok-consume-hw 0 1 100)
                  (ok-consume-hw 0 0 20)
                  (ok-consume-hw 0 1 50)])]
    (is (false? (:valid? checked)))
    (is (= 1 (count (:violations checked))))
    (is (= 1 (-> checked :violations first :partition)))))