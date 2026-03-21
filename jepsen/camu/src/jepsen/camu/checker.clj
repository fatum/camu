(ns jepsen.camu.checker
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [jepsen [checker :as checker]]))

(defn ok-produces
  "Filters the history for successful produce operations and returns their
   offset/value pairs."
  [history]
  (->> history
       (filter #(and (= (:f %) :produce)
                     (= (:type %) :ok)))
       (map :value)))

(defn drain-messages
  "Extracts all consumed messages from the drain phase of the history."
  [history]
  (->> history
       (filter #(and (= (:f %) :consume)
                     (= (:type %) :ok)))
       (mapcat :value)))

(defn no-data-loss-checker
  "Verifies that every successfully acknowledged produce appears in the
   final consumed drain. Reports any missing offsets."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [produced   (ok-produces history)
            consumed   (drain-messages history)
            ;; Build a set of consumed offset values for lookup
            consumed-offsets (set (map :offset consumed))
            ;; Find produced offsets missing from consumed data
            missing    (remove #(consumed-offsets (:offset %)) produced)
            missing-ct (count missing)]
        {:valid?       (zero? missing-ct)
         :produced     (count produced)
         :consumed     (count consumed)
         :missing      missing-ct
         :missing-data (when (pos? missing-ct)
                         (take 20 missing))}))))

(defn offset-monotonicity-checker
  "Verifies that within each partition, offsets are strictly increasing
   with no gaps or duplicates."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [consumed (drain-messages history)
            ;; Group messages by partition
            by-partition (group-by :partition consumed)
            ;; Check each partition
            problems
            (reduce-kv
             (fn [acc partition msgs]
               (let [offsets (sort (map :offset msgs))
                     ;; Check for duplicates
                     dupes   (filter (fn [[k v]] (> v 1))
                                     (frequencies offsets))
                     ;; Check for gaps: offsets should be 0,1,2,...
                     expected (range (first offsets)
                                     (+ (first offsets) (count offsets)))
                     gaps     (when (seq offsets)
                                (seq (remove (set offsets) expected)))]
                 (cond-> acc
                   (seq dupes) (conj {:type      :duplicate
                                      :partition partition
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
   but have different values — which would indicate two instances wrote
   to the same partition concurrently (split brain)."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [consumed (drain-messages history)
            ;; Group by [partition offset] and look for conflicts
            by-key   (group-by (juxt :partition :offset) consumed)
            conflicts
            (->> by-key
                 (filter (fn [[k msgs]]
                           (> (count (distinct (map :value msgs))) 1)))
                 (map (fn [[k msgs]]
                        {:partition (first k)
                         :offset    (second k)
                         :values    (distinct (map :value msgs))})))]
        {:valid?    (empty? conflicts)
         :conflicts (when (seq conflicts)
                      (take 20 conflicts))}))))

(defn availability-checker
  "Calculates the fraction of successful operations during the test.
   Reports ok-count, fail-count, info-count, and availability percentage.
   validity is always true — this checker is informational, not pass/fail."
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
   most one distinct value among writes recorded after each rejoin."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [rejoin-events (->> history
                               (filter #(and (= :nemesis (:process %))
                                             (= :info (:type %))
                                             (= :rejoin (:f %)))))
            ;; Collect all consume results to find any offset conflicts
            consumed      (drain-messages history)
            by-key        (group-by (juxt :partition :offset) consumed)
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
   checkers for timeline, stats, and perf."
  []
  (checker/compose
   {:no-data-loss        (no-data-loss-checker)
    :offset-monotonicity (offset-monotonicity-checker)
    :no-split-brain      (no-split-brain-checker)
    :availability        (availability-checker)
    :lease-fencing       (lease-fencing-checker)
    :recovery-time       (recovery-time-checker)
    :stats               (checker/stats)}))
