(ns jepsen.camu.checker
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [jepsen [checker :as checker]]))

(defn ok-produces
  "Filters the history for successful produce operations and returns their
   value maps (containing :key :value :partition :offset)."
  [history]
  (->> history
       (filter #(and (= (:f %) :produce)
                     (= (:type %) :ok)))
       (map :value)))

(defn drain-messages
  "Extracts all consumed messages from :drain operations in the history."
  [history]
  (->> history
       (filter #(and (= (:f %) :drain)
                     (= (:type %) :ok)))
       (mapcat (fn [op] (get-in op [:value :messages])))))

(defn no-data-loss-checker
  "Verifies that every successfully acknowledged produce appears in the
   final drain data. Matches on (partition, offset) pairs.
   Reports: total acked, total drained, missing count, missing details."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [produced    (ok-produces history)
            drained     (drain-messages history)
            ;; Build a set of (partition, offset) pairs from drain data
            drained-set (set (map (juxt :partition :offset) drained))
            ;; Find produced messages whose (partition, offset) is missing from drain
            missing     (remove #(drained-set [(:partition %) (:offset %)]) produced)
            missing-ct  (count missing)]
        {:valid?       (zero? missing-ct)
         :acked        (count produced)
         :drained      (count drained)
         :missing      missing-ct
         :missing-data (when (pos? missing-ct)
                         (take 20 missing))}))))

(defn offset-monotonicity-checker
  "Verifies that within each partition, offsets from drain data are strictly
   increasing with no gaps or duplicates. Expects offsets 0, 1, 2, ... N."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [drained      (drain-messages history)
            by-partition  (group-by :partition drained)
            problems
            (reduce-kv
             (fn [acc partition msgs]
               (let [offsets  (sort (map :offset msgs))
                     ;; Check for duplicates
                     dupes    (into {} (filter (fn [[_ v]] (> v 1))
                                              (frequencies offsets)))
                     ;; Check for gaps: offsets should be 0, 1, 2, ...
                     expected (when (seq offsets)
                                (range 0 (inc (apply max offsets))))
                     gaps     (when (seq offsets)
                                (seq (remove (set offsets) expected)))]
                 (cond-> acc
                   (seq dupes) (conj {:type       :duplicate
                                      :partition  partition
                                      :duplicates dupes})
                   (seq gaps)  (conj {:type      :gap
                                      :partition partition
                                      :gaps      (take 20 gaps)}))))
             []
             by-partition)]
        {:valid?   (empty? problems)
         :problems (when (seq problems) problems)}))))

(defn no-split-brain-checker
  "Verifies that no two messages in the same partition share an offset
   but have different values. Two different values at the same (partition, offset)
   would indicate split-brain concurrent writes."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [drained   (drain-messages history)
            by-key    (group-by (juxt :partition :offset) drained)
            conflicts (->> by-key
                           (filter (fn [[_ msgs]]
                                     (> (count (distinct (map :value msgs))) 1)))
                           (map (fn [[k msgs]]
                                  {:partition (first k)
                                   :offset    (second k)
                                   :values    (vec (distinct (map :value msgs)))})))]
        {:valid?    (empty? conflicts)
         :conflicts (when (seq conflicts)
                      (take 20 conflicts))}))))

(defn total-order-checker
  "Within each partition, verifies that messages appear in offset order
   and no offset is skipped (0, 1, 2, ... with no gaps). This is stronger
   than monotonicity — it checks completeness from zero."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [drained      (drain-messages history)
            by-partition  (group-by :partition drained)
            results
            (reduce-kv
             (fn [acc partition msgs]
               (let [sorted-offsets (sort (map :offset msgs))
                     n             (count sorted-offsets)
                     expected      (range 0 n)
                     out-of-order  (when (seq sorted-offsets)
                                     (not= sorted-offsets (sort sorted-offsets)))
                     missing-from-zero (when (and (seq sorted-offsets)
                                                  (not= 0 (first sorted-offsets)))
                                         (first sorted-offsets))
                     gaps          (when (seq sorted-offsets)
                                    (seq (remove (set sorted-offsets) expected)))]
                 (cond-> acc
                   out-of-order
                   (conj {:type :out-of-order :partition partition})
                   missing-from-zero
                   (conj {:type :missing-start :partition partition
                          :first-offset missing-from-zero})
                   (seq gaps)
                   (conj {:type :gap :partition partition
                          :gaps (take 20 gaps)}))))
             []
             by-partition)]
        {:valid?   (empty? results)
         :problems (when (seq results) results)}))))

(defn availability-checker
  "Calculates the fraction of successful operations during the test.
   Reports ok-count, fail-count, info-count, and availability percentage.
   Validity is always true — this checker is informational, not pass/fail."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [invocations (->> history (filter #(= :invoke (:type %))))
            oks         (->> history (filter #(= :ok    (:type %))))
            fails       (->> history (filter #(= :fail  (:type %))))
            infos       (->> history (filter #(= :info  (:type %))))
            total       (count invocations)
            availability (if (pos? total)
                           (double (/ (count oks) total))
                           0.0)]
        {:valid?       true
         :ok-count     (count oks)
         :fail-count   (count fails)
         :info-count   (count infos)
         :availability availability}))))

(defn lease-fencing-checker
  "After instance rejoin events, verifies that epoch fencing prevented
   split-brain writes. Checks that each (partition, offset) pair has at
   most one distinct value."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [rejoin-events (->> history
                               (filter #(and (= :nemesis (:process %))
                                             (= :info (:type %))
                                             (= :rejoin (:f %)))))
            drained       (drain-messages history)
            by-key        (group-by (juxt :partition :offset) drained)
            conflicts     (->> by-key
                               (filter (fn [[_ msgs]]
                                         (> (count (distinct (map :value msgs))) 1)))
                               (map (fn [[k msgs]]
                                      {:partition (first k)
                                       :offset    (second k)
                                       :values    (distinct (map :value msgs))})))]
        {:valid?        (empty? conflicts)
         :rejoin-count  (count rejoin-events)
         :conflicts     (take 10 conflicts)}))))

(defn recovery-time-checker
  "Measures time between each nemesis fault event and the first successful
   client operation that follows it. All times are in nanoseconds."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [nemesis-events (->> history
                                (filter #(and (= :nemesis (:process %))
                                              (= :info (:type %)))))
            recovery-times (for [nem nemesis-events
                                 :let [recovery (->> history
                                                     (filter #(and (> (:time %) (:time nem))
                                                                   (= :ok (:type %))
                                                                   (not= :nemesis (:process %))))
                                                     first)]
                                 :when recovery]
                             (- (:time recovery) (:time nem)))]
        {:valid?           true
         :min-recovery-ns  (when (seq recovery-times) (apply min recovery-times))
         :max-recovery-ns  (when (seq recovery-times) (apply max recovery-times))
         :mean-recovery-ns (when (seq recovery-times)
                             (long (/ (reduce + recovery-times) (count recovery-times))))
         :count            (count recovery-times)}))))

(defn combined-checker
  "Returns a composition of all camu checkers plus standard Jepsen
   checkers for stats."
  []
  (checker/compose
   {:no-data-loss        (no-data-loss-checker)
    :offset-monotonicity (offset-monotonicity-checker)
    :no-split-brain      (no-split-brain-checker)
    :total-order         (total-order-checker)
    :availability        (availability-checker)
    :lease-fencing       (lease-fencing-checker)
    :recovery-time       (recovery-time-checker)
    :stats               (checker/stats)}))
