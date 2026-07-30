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