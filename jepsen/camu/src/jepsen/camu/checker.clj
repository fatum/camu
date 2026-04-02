(ns jepsen.camu.checker
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [jepsen [checker :as checker]]))

(defn unhandled-exception-checker
  "Wraps Jepsen's unhandled-exceptions checker but treats any recorded
   exception as a test failure."
  []
  (let [inner (checker/unhandled-exceptions)]
    (reify checker/Checker
      (check [_ test history opts]
        (let [result     (checker/check inner test history opts)
              exceptions (:exceptions result)]
          (assoc result :valid? (empty? exceptions)))))))

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

(defn- drain-ops
  "Extracts all operations for the given final verification function."
  [history f]
  (->> history
       (filter #(= (:f %) f))))

(defn drain-coverage-checker
  "Requires one successful final drain op per partition. Fails if any partition
   is missing coverage, or if any drain op failed."
  [f]
  (reify checker/Checker
    (check [_ test history opts]
      (let [expected      (set (range (:num-partitions test)))
            ops           (drain-ops history f)
            successful    (->> ops
                               (filter #(= (:type %) :ok))
                               (keep #(get-in % [:value :partition]))
                               set)
            failures      (->> ops
                               (filter #(= (:type %) :fail))
                               (map (fn [op]
                                      {:partition (get-in op [:value :partition])
                                       :error     (:error op)}))
                               vec)
            missing       (set/difference expected successful)
            unexpected    (set/difference successful expected)]
        {:valid?               (and (empty? missing)
                                    (empty? unexpected)
                                    (empty? failures))
         :expected-partitions  (count expected)
         :successful-partitions (count successful)
         :missing-partitions   (when (seq missing) (vec (sort missing)))
         :unexpected-partitions (when (seq unexpected) (vec (sort unexpected)))
         :failures             (when (seq failures) (take 20 failures))}))))

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
               (let [raw-offsets   (mapv :offset msgs)
                     sorted-offsets (sort raw-offsets)
                     n             (count sorted-offsets)
                     expected      (range 0 n)
                     ;; Check if drain returned messages out of offset order
                     out-of-order  (when (seq raw-offsets)
                                     (not= raw-offsets (vec sorted-offsets)))
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
  "Calculates the fraction of successful client operations during the test.
   Excludes nemesis events. Uses completions (ok + fail + info) as denominator.
   Informational only — always valid."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [client-ops  (->> history
                             (filter #(not= :nemesis (:process %)))
                             (filter #(#{:ok :fail :info} (:type %))))
            oks         (->> client-ops (filter #(= :ok   (:type %))))
            fails       (->> client-ops (filter #(= :fail (:type %))))
            infos       (->> client-ops (filter #(= :info (:type %))))
            ok-ct       (count oks)
            fail-ct     (count fails)
            info-ct     (count infos)
            total       (+ ok-ct fail-ct info-ct)
            availability (if (pos? total)
                           (double (/ ok-ct total))
                           0.0)]
        {:valid?       true
         :ok-count     ok-ct
         :fail-count   fail-ct
         :info-count   info-ct
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

(defn replica-drain-messages
  "Extracts all consumed messages from :replica-drain operations in the history."
  [history]
  (->> history
       (filter #(and (= (:f %) :replica-drain)
                     (= (:type %) :ok)))
       (mapcat (fn [op] (get-in op [:value :messages])))))

(defn replica-convergence-checker
  "Verifies that every acked produce is eventually readable from replicas after
   recovery. Compares acked keys against keys returned by :replica-drain ops."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [produced-keys (->> (ok-produces history) (map :key) set)
            replica-keys  (->> (replica-drain-messages history)
                               (map :key)
                               set)
            leader-keys   (->> (drain-messages history)
                               (map :key)
                               set)
            missing-from-replicas (set/difference produced-keys replica-keys)
            ;; Keys present on leader but missing from replicas indicate
            ;; replication lag that wasn't resolved after recovery.
            leader-only           (set/difference leader-keys replica-keys)]
        {:valid?               (empty? missing-from-replicas)
         :acked-keys           (count produced-keys)
         :replica-drained-keys (count replica-keys)
         :leader-drained-keys  (count leader-keys)
         :missing-from-replicas (count missing-from-replicas)
         :leader-only          (count leader-only)
         :missing-data         (when (seq missing-from-replicas)
                                 (take 20 (sort missing-from-replicas)))}))))

(defn recovery-time-checker
  "Measures time between each nemesis fault-start event and the first successful
   client operation that follows it. Only :start events are meaningful —
   :stop events are recovery actions, not faults. All times in nanoseconds."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [fault-starts (->> history
                              (filter #(and (= :nemesis (:process %))
                                            (= :info (:type %))))
                              ;; Only measure from fault injections, not recoveries
                              (filter #(let [v (:value %)]
                                         (or (= :start v)
                                             (and (vector? v)
                                                  (#{:killed :paused :s3-blocked :rejoined
                                                     :left :membership-cycle}
                                                   (first v)))))))
            recovery-times (for [nem fault-starts
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

(defn committed-durability-checker
  "Matches acked produces to drained messages by message KEY. This is the
   primary safety checker for replicated topics — keys are globally unique
   so we don't depend on partition/offset stability across leader changes."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [produced-keys (->> (ok-produces history) (map :key) set)
            drained-keys  (->> (drain-messages history)
                               (map :key)
                               set)
            lost          (set/difference produced-keys drained-keys)]
        {:valid?       (empty? lost)
         :acked-keys   (count produced-keys)
         :drained-keys (count drained-keys)
         :lost         (count lost)
         :lost-data    (when (seq lost) (take 20 (sort lost)))}))))

(defn no-ghost-reads-checker
  "Verifies the consumer never sees data above the reported high watermark.
   For each :ok :consume op with a high-watermark, checks that no message
   offset exceeds the HW."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [violations (->> history
                            (filter #(and (= (:f %) :consume)
                                          (= (:type %) :ok)))
                            (keep (fn [op]
                                    (let [hw   (get-in op [:value :high-watermark])
                                          msgs (get-in op [:value :messages])]
                                      (when (and hw (seq msgs))
                                        (let [above (filter #(>= (:offset %) hw) msgs)]
                                          (when (seq above)
                                            {:process   (:process op)
                                             :partition (get-in op [:value :partition])
                                             :hw        hw
                                             :offsets   (mapv :offset above)})))))))]
        {:valid?     (empty? violations)
         :violations (when (seq violations) (take 20 violations))}))))

(defn single-leader-checker
  "Epoch-based leader uniqueness: for each (partition, leader-epoch) pair,
   only one node should have acked writes. Two different nodes acking in
   the same epoch indicates a split-brain."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [produces  (->> history
                           (filter #(and (= (:f %) :produce)
                                         (= (:type %) :ok)))
                           (filter #(get-in % [:value :leader-epoch])))
            grouped   (group-by (fn [op]
                                  [(get-in op [:value :partition])
                                   (get-in op [:value :leader-epoch])])
                                produces)
            conflicts (->> grouped
                           (keep (fn [[[part epoch] ops]]
                                   (let [nodes (distinct (map #(get-in % [:value :node]) ops))]
                                     (when (> (count nodes) 1)
                                       {:partition    part
                                        :leader-epoch epoch
                                        :nodes        (vec nodes)
                                        :count        (count ops)})))))]
        {:valid?    (empty? conflicts)
         :checked   (count grouped)
         :conflicts (when (seq conflicts) (take 20 conflicts))}))))

(defn hw-monotonicity-checker
  "High watermark should never decrease per partition per consumer process.
   Checks that successive :ok :consume ops on the same
   (process, partition, serving-node) have non-decreasing high-watermark
   values. This avoids false positives during routing disagreement windows,
   where a client in leader-read mode can retry from a dead leader to a lagging
   follower before the new leader is universally advertised."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [consumes (->> history
                          (filter #(and (= (:f %) :consume)
                                        (= (:type %) :ok)
                                        (get-in % [:value :high-watermark]))))
            ;; Group by [process, partition, serving node]
            grouped  (group-by (fn [op]
                                 [(:process op)
                                  (get-in op [:value :partition])
                                  (get-in op [:value :node])])
                               consumes)
            violations
            (reduce-kv
             (fn [acc [proc part node] ops]
               (let [hws (map #(get-in % [:value :high-watermark]) ops)]
                 (if (apply <= hws)
                   acc
                   (conj acc {:process   proc
                              :partition part
                              :node      node
                              :hws       (take 20 hws)}))))
             []
             grouped)]
        {:valid?     (empty? violations)
         :checked    (count grouped)
         :violations (when (seq violations) (take 20 violations))}))))

(defn truncation-safety-checker
  "Like committed-durability but tracks partition and offset for diagnostics.
   Reports acked writes not found in drain, grouped by partition."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [produced    (ok-produces history)
            drained     (drain-messages history)
            drained-set (set (map :key drained))
            missing     (remove #(drained-set (:key %)) produced)
            by-part     (group-by :partition missing)]
        {:valid?       (zero? (count missing))
         :acked        (count produced)
         :drained      (count drained)
         :missing      (count missing)
         :by-partition (when (seq by-part)
                         (reduce-kv (fn [m p msgs]
                                      (assoc m p {:count   (count msgs)
                                                  :offsets (take 10 (sort (map :offset msgs)))
                                                  :keys    (take 10 (sort (map :key msgs)))}))
                                    {}
                                    by-part))}))))

(defn exactly-once-checker
  "Verifies exactly-once delivery for idempotent produce:
   1. Every confirmed (:ok) key appears in drain exactly once (no duplicates).
   2. Every confirmed key appears in drain (no data loss).
   3. No key in drain was never attempted — keys from :info (indeterminate)
      ops are allowed in drain since the write may have succeeded."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [;; Keys confirmed by the client (:ok).
            acked-keys    (->> history
                               (filter #(and (= (:f %) :idempotent-produce)
                                             (= (:type %) :ok)))
                               (map #(get-in % [:value :key]))
                               set)
            ;; Keys from indeterminate ops (:info) — may or may not have been written.
            inflight-keys (->> history
                               (filter #(and (= (:f %) :idempotent-produce)
                                             (= (:type %) :info)))
                               (map #(get-in % [:value :key]))
                               set)
            drained       (drain-messages history)
            ;; Only consider keys matching the idempotent produce pattern (k-N).
            drained-keys  (->> (map :key drained)
                               (filter #(and (some? %) (.startsWith ^String % "k-"))))
            drained-set   (set drained-keys)
            key-freq      (frequencies drained-keys)
            duplicate-keys (->> key-freq
                                (filter (fn [[_ cnt]] (> cnt 1)))
                                (into {}))
            ;; Missing: acked but not in drain = data loss.
            missing       (set/difference acked-keys drained-set)
            ;; Ghosts: in drain but never acked AND never inflight.
            ;; Inflight keys appearing in drain is expected (write succeeded,
            ;; response was lost).
            ghosts        (set/difference drained-set (set/union acked-keys inflight-keys))]
        {:valid?          (and (empty? duplicate-keys)
                               (empty? missing)
                               (empty? ghosts))
         :acked-count     (count acked-keys)
         :inflight-count  (count inflight-keys)
         :drained-count   (count drained-keys)
         :duplicate-count (count duplicate-keys)
         :duplicates      (when (seq duplicate-keys) (take 20 duplicate-keys))
         :missing-count   (count missing)
         :missing         (when (seq missing) (take 20 missing))
         :ghost-count     (count ghosts)
         :ghosts          (when (seq ghosts) (take 20 ghosts))}))))

(defn- parse-partition-key
  [k]
  (cond
    (integer? k) k
    (string? k) (Integer/parseInt k)
    (keyword? k) (Integer/parseInt (name k))
    :else nil))

(defn consumer-offset-checker
  "Checks explicit consumer offset semantics for the offsets workload:
   committed offsets must not move backwards, must not exceed what that client
   consumed for a partition, and fetched offsets must not exceed the latest
   successfully committed value."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [events (sort-by :time history)
            result (reduce
                    (fn [{:keys [seen committed observed violations] :as acc} op]
                      (let [proc (:process op)
                            value (:value op)]
                        (cond
                          (and (= :consume (:f op))
                               (= :ok (:type op))
                               (seq (:messages value)))
                          (let [partition (:partition value)
                                next-offset (inc (apply max (map :offset (:messages value))))]
                            (update-in acc [:seen proc partition] #(max (or % 0) next-offset)))

                          (and (= :commit-offsets (:f op))
                               (= :ok (:type op)))
                          (let [{:keys [consumer-id partition offset]} value
                                max-seen (get-in seen [proc partition] 0)
                                prev-committed (get-in committed [consumer-id partition] 0)
                                violations' (cond-> violations
                                              (> offset max-seen)
                                              (conj {:type :commit-ahead-of-consume
                                                     :consumer-id consumer-id
                                                     :process proc
                                                     :partition partition
                                                     :offset offset
                                                     :max-seen max-seen})
                                              (< offset prev-committed)
                                              (conj {:type :commit-regressed
                                                     :consumer-id consumer-id
                                                     :partition partition
                                                     :offset offset
                                                     :prev-committed prev-committed}))]
                            (-> acc
                                (assoc :violations violations')
                                (assoc-in [:committed consumer-id partition]
                                          (max prev-committed offset))))

                          (and (= :get-offsets (:f op))
                               (= :ok (:type op)))
                          (let [{:keys [consumer-id offsets]} value
                                [observed' violations']
                                (reduce-kv
                                 (fn [[observed violations] raw-partition offset]
                                   (let [partition (parse-partition-key raw-partition)
                                         offset (long offset)
                                         prev-observed (get-in observed [consumer-id partition] 0)
                                         max-committed (get-in committed [consumer-id partition] 0)]
                                     [(assoc-in observed [consumer-id partition]
                                                (max prev-observed offset))
                                      (cond-> violations
                                        (< offset prev-observed)
                                        (conj {:type :fetched-offset-regressed
                                               :consumer-id consumer-id
                                               :partition partition
                                               :offset offset
                                               :prev-observed prev-observed})
                                        (> offset max-committed)
                                        (conj {:type :fetched-offset-ahead-of-commit
                                               :consumer-id consumer-id
                                               :partition partition
                                               :offset offset
                                               :max-committed max-committed}))]))
                                 [observed violations]
                                 offsets)]
                            (-> acc
                                (assoc :observed observed')
                                (assoc :violations violations')))

                          :else acc)))
                    {:seen {} :committed {} :observed {} :violations []}
                    events)]
        {:valid?     (empty? (:violations result))
         :violations (when (seq (:violations result))
                       (take 20 (:violations result)))}))))

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
