(ns jepsen.camu.nemesis
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [nemesis :as nemesis]
                    [generator :as gen]
                    [control :as c]]
            [jepsen.nemesis.combined :as nc]))

(defn kill-camu!
  "Kills the camu process via SIGKILL on the current node."
  []
  (c/exec :pkill :-9 :-f "camu" (c/lit "|| true")))

(defn start-camu!
  "Starts camu on the current node (assumes process is not running)."
  []
  (c/exec :bash :-c
          (str "nohup /opt/camu/camu serve --config /etc/camu/camu.yaml "
               ">> /var/log/camu.log 2>&1 &"))
  (Thread/sleep 1000))

(defn pause-camu!
  "Sends SIGSTOP to camu on the current node."
  []
  (c/exec :pkill :-STOP :-f "camu" (c/lit "|| true")))

(defn resume-camu!
  "Sends SIGCONT to camu on the current node."
  []
  (c/exec :pkill :-CONT :-f "camu" (c/lit "|| true")))

(defn stop-camu!
  "Gracefully stops camu via SIGTERM on the current node.
   Waits up to 5s for the process to exit cleanly."
  []
  (c/exec :pkill :-TERM :-f "camu" (c/lit "|| true"))
  ;; Wait for graceful shutdown (WAL flush + deregister)
  (Thread/sleep 5000))

(defn block-s3!
  "Blocks traffic to MinIO port 9000 via iptables on the current node."
  [s3-host]
  (c/exec :iptables :-A :OUTPUT :-d s3-host :-p :tcp :--dport 9000 :-j :DROP
          (c/lit "|| true")))

(defn unblock-s3!
  "Removes the iptables rule blocking MinIO port 9000."
  [s3-host]
  (c/exec :iptables :-D :OUTPUT :-d s3-host :-p :tcp :--dport 9000 :-j :DROP
          (c/lit "|| true")))

(defn kill-nemesis
  "A nemesis that kills and restarts camu processes.
   Tracks killed nodes; :stop restarts only those that were killed."
  []
  (let [killed (atom #{})]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (case (:value op)
          :start (let [node (rand-nth (:nodes test))]
                   (c/on-nodes test [node] (fn [_ _] (kill-camu!)))
                   (swap! killed conj node)
                   (assoc op :value [:killed node]))
          :stop  (let [to-restart (vec @killed)]
                   (reset! killed #{})
                   (when (seq to-restart)
                     ;; Wait for killed processes to fully release ports
                     (Thread/sleep 3000)
                     (doseq [node to-restart]
                       (try
                         (c/on-nodes test [node] (fn [_ _] (start-camu!)))
                         (catch Exception e
                           (info "Failed to restart" node (.getMessage e))))))
                   (assoc op :value [:restarted to-restart]))))
      (teardown! [this test]
        (let [to-restart (vec @killed)]
          (when (seq to-restart)
            (Thread/sleep 3000)
            (doseq [node to-restart]
              (try
                (c/on-nodes test [node] (fn [_ _] (start-camu!)))
                (catch Exception _)))))))))

(defn pause-nemesis
  "A nemesis that pauses and resumes camu processes."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (let [node (rand-nth (:nodes test))]
        (case (:value op)
          :start (do (c/on-nodes test [node] (fn [_ _] (pause-camu!)))
                     (assoc op :value [:paused node]))
          :stop  (do (c/on-nodes test (:nodes test) (fn [_ _] (resume-camu!)))
                     (assoc op :value :resumed)))))
    (teardown! [this test]
      (c/on-nodes test (:nodes test) (fn [_ _] (resume-camu!))))))

(defn partition-nemesis
  "A nemesis that partitions the network into random halves."
  []
  (nemesis/partition-random-halves))

(defn rejoin-nemesis
  "A nemesis that kills a node, waits for lease expiry, then restarts it."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:value op)
        :start
        (let [node (rand-nth (:nodes test))]
          (info "Rejoin nemesis: killing" node "for lease expiry test")
          (c/on-nodes test [node] (fn [_ _] (kill-camu!)))
          (Thread/sleep 20000)
          (c/on-nodes test [node] (fn [_ _] (start-camu!)))
          (assoc op :value [:rejoined node]))
        :stop
        (assoc op :value :no-op)))
    (teardown! [this test])))

(defn leave-nemesis
  "A nemesis that gracefully stops a node (SIGTERM) and restarts it.
   Unlike kill-nemesis, this allows the node to flush WAL and deregister
   from the cluster, so partition reassignment happens immediately.
   :start = graceful stop (leave), :stop = restart (join)."
  []
  (let [stopped (atom #{})]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (case (:value op)
          :start (let [node (rand-nth (:nodes test))]
                   (info "Leave nemesis: gracefully stopping" node)
                   (c/on-nodes test [node] (fn [_ _] (stop-camu!)))
                   (swap! stopped conj node)
                   (assoc op :value [:left node]))
          :stop  (let [to-start (vec @stopped)]
                   (reset! stopped #{})
                   (when (seq to-start)
                     (doseq [node to-start]
                       (try
                         (info "Join nemesis: starting" node)
                         (c/on-nodes test [node] (fn [_ _] (start-camu!)))
                         (catch Exception e
                           (info "Failed to start" node (.getMessage e))))))
                   (assoc op :value [:joined to-start]))))
      (teardown! [this test]
        (let [to-start (vec @stopped)]
          (when (seq to-start)
            (doseq [node to-start]
              (try
                (c/on-nodes test [node] (fn [_ _] (start-camu!)))
                (catch Exception _)))))))))

(defn membership-nemesis
  "A nemesis that exercises cluster membership changes: gracefully removes
   a node, waits for rebalance, then adds it back. The full cycle is a
   single :start event so the generator can control pacing.
   Tests: partition reassignment on leave, rebalance on join, no data loss."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:value op)
        :start
        (let [node (rand-nth (:nodes test))]
          (info "Membership nemesis: removing" node "from cluster")
          (c/on-nodes test [node] (fn [_ _] (stop-camu!)))
          ;; Wait for rebalance to redistribute partitions
          (info "Membership nemesis: waiting 15s for rebalance...")
          (Thread/sleep 15000)
          ;; Rejoin
          (info "Membership nemesis: adding" node "back to cluster")
          (c/on-nodes test [node] (fn [_ _] (start-camu!)))
          ;; Wait for join rebalance
          (Thread/sleep 10000)
          (assoc op :value [:membership-cycle node]))
        :stop
        (assoc op :value :no-op)))
    (teardown! [this test]
      ;; Ensure all nodes are running
      (c/on-nodes test (:nodes test)
                  (fn [_ _]
                    (start-camu!))))))

(defn s3-partition-nemesis
  "A nemesis that blocks/unblocks a random node's access to MinIO port 9000."
  [s3-host]
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (let [node (rand-nth (:nodes test))]
        (case (:value op)
          :start (do (c/on-nodes test [node] (fn [_ _] (block-s3! s3-host)))
                     (assoc op :value [:s3-blocked node]))
          :stop  (do (c/on-nodes test (:nodes test) (fn [_ _] (unblock-s3! s3-host)))
                     (assoc op :value :s3-unblocked)))))
    (teardown! [this test]
      (c/on-nodes test (:nodes test) (fn [_ _] (unblock-s3! s3-host))))))

(defn clock-skew-nemesis
  "A nemesis that introduces clock drift on nodes."
  []
  (nemesis/clock-scrambler 10))

(defn composed-nemesis
  "Returns a nemesis that composes fault types specified in the faults set.
   Supported fault keys: :kill :partition :pause :rejoin :leave :membership
                         :s3-partition :clock-skew

   For :kill — start = SIGKILL process, stop = restart process
   For :leave — start = graceful SIGTERM (deregister), stop = restart (rejoin)
   For :membership — start = full leave/wait/rejoin cycle, stop = no-op
   For :partition — start = partition network, stop = heal network
   For :pause — start = SIGSTOP, stop = SIGCONT"
  ([] (composed-nemesis #{:kill :partition :pause}))
  ([faults]
   (nemesis/compose
    (cond-> {}
      (:kill faults)
      (assoc #{:kill} (kill-nemesis))

      (:partition faults)
      (assoc #{:partition} (partition-nemesis))

      (:pause faults)
      (assoc #{:pause} (pause-nemesis))

      (:rejoin faults)
      (assoc #{:rejoin} (rejoin-nemesis))

      (:leave faults)
      (assoc #{:leave} (leave-nemesis))

      (:membership faults)
      (assoc #{:membership} (membership-nemesis))

      (:s3-partition faults)
      (assoc #{:s3-partition} (s3-partition-nemesis "minio"))

      (:clock-skew faults)
      (assoc #{:clock-skew} (clock-skew-nemesis))))))

(defn fault-cycle
  "Returns a gen/cycle for a single fault type with appropriate timing.
   :membership has a longer cycle since the start event includes the full
   leave/wait/rejoin internally."
  [fault]
  (case fault
    :membership (gen/cycle
                 [(gen/sleep 10)
                  {:type :info :f fault :value :start}
                  (gen/sleep 5)
                  {:type :info :f fault :value :stop}])
    ;; Default: 5s quiet, inject fault, 10s active, stop fault
    (gen/cycle
     [(gen/sleep 5)
      {:type :info :f fault :value :start}
      (gen/sleep 10)
      {:type :info :f fault :value :stop}])))

(defn fault-cycles
  "Returns a sequence of gen/cycle generators for the given fault keys."
  [faults]
  (map fault-cycle faults))

(defn nemesis-generator
  "Returns a generator that injects only the requested faults with ~5s stagger."
  ([time-limit] (nemesis-generator time-limit #{:kill :partition :pause}))
  ([time-limit faults]
   (gen/phases
    ;; Active phase: interleave requested fault types
    (->> (gen/mix (fault-cycles faults))
         (gen/stagger 5)
         (gen/time-limit time-limit))
    ;; Recovery: stop all active fault types
    (apply gen/phases
           (for [fault faults]
             (gen/nemesis (gen/once {:type :info :f fault :value :stop}))))
    ;; Let the cluster stabilize
    (gen/sleep 15))))
