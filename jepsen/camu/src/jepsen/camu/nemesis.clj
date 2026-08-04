(ns jepsen.camu.nemesis
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [nemesis :as nemesis]
                    [generator :as gen]
                    [control :as c]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.combined :as nc]
            [jepsen.camu.client :as client]
            [jepsen.camu.db :as db]))

(defn signal-camu!
  [signal]
  (c/exec :bash :-lc
          (str "if [ -f " db/camu-pid " ]; then "
               "kill -" signal " $(cat " db/camu-pid ") >/dev/null 2>&1 || true; "
               "fi")))

(defn kill-camu!
  "Kills the camu process via SIGKILL on the current node."
  []
  (signal-camu! "KILL"))

(defn start-camu!
  "Starts camu on the current node if it is not already running."
  []
  (let [running? (try
                   (c/exec :bash :-lc
                           (str "if [ -f " db/camu-pid " ]; then "
                                "kill -0 $(cat " db/camu-pid ") >/dev/null 2>&1; "
                                "else exit 1; fi"))
                   true
                   (catch Exception _ false))]
    (when-not running?
      ;; Remove a stale pidfile before restarting so start-daemon! can write a
      ;; fresh one. Using the same daemon helper as DB setup avoids the
      ;; pgrep-against-shell-string false positive the old implementation had.
      (c/exec :rm :-f db/camu-pid)
      (cu/start-daemon!
       {:logfile db/camu-log
        :pidfile db/camu-pid
        :chdir   db/camu-data}
       db/camu-bin
       "serve" "--config" db/camu-config)
      (Thread/sleep 1000))))

(defn pause-camu!
  "Sends SIGSTOP to camu on the current node."
  []
  (signal-camu! "STOP"))

(defn resume-camu!
  "Sends SIGCONT to camu on the current node."
  []
  (signal-camu! "CONT"))

(defn stop-camu!
  "Gracefully stops camu via SIGTERM on the current node.
   Waits up to 5s for the process to exit cleanly."
  []
  (signal-camu! "TERM")
  ;; Wait for graceful shutdown (flush + deregister)
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
  (let [inner (nemesis/partition-random-halves)]
    (reify nemesis/Nemesis
      (setup! [this test]
        (nemesis/setup! inner test)
        this)
      (invoke! [this test op]
        ;; Jepsen's built-in partition nemesis expects the op function to be the
        ;; control action itself (:start/:stop), and it owns :value for the
        ;; generated grudge/heal result. This harness uses :f=:partition with
        ;; :value=:start|:stop, so strip :value before delegating.
        (nemesis/invoke! inner test (-> op
                                        (assoc :f (:value op))
                                        (dissoc :value))))
      (teardown! [this test]
        (nemesis/teardown! inner test)))))

(defn- ring-neighbor-map
  "Returns a map of node -> allowed neighbor set for a ring topology."
  [nodes]
  (let [n (count nodes)]
    (into {}
          (map-indexed
           (fn [idx node]
             [node #{(nth nodes (mod (dec idx) n))
                     (nth nodes (mod (inc idx) n))}])
           nodes))))

(defn- ring-block-map
  "Returns a map of node -> peers that should be blocked to leave only ring
   neighbor connectivity."
  [nodes]
  (let [all-nodes  (set nodes)
        neighbors  (ring-neighbor-map nodes)]
    (into {}
          (map (fn [node]
                 [node (sort (seq (disj (set/difference all-nodes (neighbors node))
                                        node)))])
               nodes))))

(defn- block-peer!
  [peer]
  (c/exec :iptables :-A :OUTPUT :-d peer :-j :DROP
          (c/lit "|| true")))

(defn- unblock-peer!
  [peer]
  (c/exec :iptables :-D :OUTPUT :-d peer :-j :DROP
          (c/lit "|| true")))

(defn partition-ring-nemesis
  "A ring-topology network partition. During the fault, each node can only talk
   to its two ring neighbors; all other node-to-node links are blocked."
  []
  (let [blocked (atom {})]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (case (:value op)
          :start
          (let [nodes     (vec (:nodes test))
                block-map (ring-block-map nodes)]
            (reset! blocked block-map)
            (doseq [[node peers] block-map]
              (c/on-nodes test [node]
                          (fn [_ _]
                            (doseq [peer peers]
                              (block-peer! peer)))))
            (assoc op :value [:isolated block-map]))
          :stop
          (let [block-map @blocked]
            (doseq [[node peers] block-map]
              (c/on-nodes test [node]
                          (fn [_ _]
                            (doseq [peer peers]
                              (unblock-peer! peer)))))
            (reset! blocked {})
            (assoc op :value :network-healed))))
      (teardown! [this test]
        (let [block-map @blocked]
          (when (seq block-map)
            (doseq [[node peers] block-map]
              (c/on-nodes test [node]
                          (fn [_ _]
                            (doseq [peer peers]
                              (unblock-peer! peer))))))
          (reset! blocked {}))))))

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
   Unlike kill-nemesis, this allows the node to flush local data and deregister
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
      ;; Ensure all nodes are running without launching duplicates.
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

(defn- address->node
  "Extracts the hostname from an address like \"n1:8080\" → \"n1\"."
  [addr]
  (when addr
    (first (clojure.string/split addr #":"))))

(defn find-leader-node
  "Queries the routing endpoint to find which node owns the most partitions
   for the given topic. Returns the node name or nil."
  [nodes topic]
  (let [node-set (set nodes)]
    (some (fn [node]
            (when-let [routing (client/get-routing! node topic)]
              (let [;; routing.partitions is a map keyed by partition ID string,
                    ;; each value has :address (e.g. "n1:8080").
                    partitions (or (:partitions routing) {})
                    leaders    (->> (vals partitions)
                                    (keep (comp address->node :address)))
                    freqs      (frequencies leaders)]
                (when (seq freqs)
                  (let [candidate (key (apply max-key val freqs))]
                    (when (node-set candidate)
                      candidate))))))
          (shuffle nodes))))

(defn leader-kill-nemesis
  "A nemesis that kills the node owning the most partitions (the busiest leader).
   Falls back to a random node if routing info is unavailable."
  []
  (let [killed (atom #{})]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (case (:value op)
          :start
          (let [topic  (:topic test)
                target (or (find-leader-node (:nodes test) topic)
                           (rand-nth (:nodes test)))]
            (info "Leader-kill nemesis: killing leader" target)
            (c/on-nodes test [target] (fn [_ _] (kill-camu!)))
            (swap! killed conj target)
            (assoc op :value [:leader-killed target]))
          :stop
          (let [to-restart (vec @killed)]
            (reset! killed #{})
            (when (seq to-restart)
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

(defn leader-pause-then-ack-nemesis
  "A nemesis that pauses the partition leader past lease expiry, lets the
   cluster elect a new leader, then resumes the stale leader. Produces that
   route to the resumed node during the window stress the stale-leader fencing
   guarantee: the node must never acknowledge a write the current ISR quorum
   does not hold. Any such ack surfaces as a committed-durability violation in
   the final drain."
  []
  (let [paused (atom #{})]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (case (:value op)
          :start
          (let [topic  (:topic test)
                target (or (find-leader-node (:nodes test) topic)
                           (rand-nth (:nodes test)))]
            (info "Leader-pause-then-ack nemesis: pausing leader" target)
            (c/on-nodes test [target] (fn [_ _] (pause-camu!)))
            (swap! paused conj target)
            ;; Hold the leader paused well past the 6s lease TTL so a new
            ;; leader is elected and the paused leader's lease expires.
            (Thread/sleep 15000)
            (info "Leader-pause-then-ack nemesis: resuming stale leader" target)
            (c/on-nodes test [target] (fn [_ _] (resume-camu!)))
            (assoc op :value [:leader-paused-resumed target]))
          :stop
          (let [to-resume (vec @paused)]
            (reset! paused #{})
            (doseq [node to-resume]
              (c/on-nodes test [node] (fn [_ _] (resume-camu!))))
            (assoc op :value [:resumed to-resume]))))
      (teardown! [this test]
        (let [to-resume (vec @paused)]
          (when (seq to-resume)
            (doseq [node to-resume]
              (c/on-nodes test [node] (fn [_ _] (resume-camu!))))))))))

(defn composed-nemesis
  "Returns a nemesis that composes fault types specified in the faults set.
   Supported fault keys: :kill :partition :pause :rejoin :leave :membership
                         :s3-partition :clock-skew :leader-kill :partition-ring
                         :leader-pause-then-ack

   For :kill — start = SIGKILL process, stop = restart process
   For :leave — start = graceful SIGTERM (deregister), stop = restart (rejoin)
   For :membership — start = full leave/wait/rejoin cycle, stop = no-op
   For :partition — start = partition network, stop = heal network
   For :pause — start = SIGSTOP, stop = SIGCONT
   For :leader-pause-then-ack — start = pause leader past lease TTL, resume stale
   leader; stop = ensure all resumed"
  ([] (composed-nemesis #{:kill :partition :pause}))
  ([faults]
   (nemesis/compose
    (cond-> {}
      (:kill faults)
      (assoc #{:kill} (kill-nemesis))

      (:partition faults)
      (assoc #{:partition} (partition-nemesis))

      (:partition-ring faults)
      (assoc #{:partition-ring} (partition-ring-nemesis))

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
      (assoc #{:clock-skew} (clock-skew-nemesis))

      (:leader-kill faults)
      (assoc #{:leader-kill} (leader-kill-nemesis))

      (:leader-pause-then-ack faults)
      (assoc #{:leader-pause-then-ack} (leader-pause-then-ack-nemesis))))))

(defn fault-cycle
  "Returns a gen/cycle for a single fault type with appropriate timing.
   :membership has a longer cycle since the start event includes the full
   leave/wait/rejoin internally."
  [fault]
  (case fault
    :partition-ring (gen/cycle
                     [(gen/sleep 5)
                      {:type :info :f fault :value :start}
                      (gen/sleep 10)
                      {:type :info :f fault :value :stop}])
    :membership (gen/cycle
                 [(gen/sleep 10)
                  {:type :info :f fault :value :start}
                  (gen/sleep 5)
                  {:type :info :f fault :value :stop}])
    :leader-kill (gen/cycle
                  [(gen/sleep 8)
                   {:type :info :f fault :value :start}
                   (gen/sleep 20)
                   {:type :info :f fault :value :stop}])
    :leader-pause-then-ack (gen/cycle
                            [(gen/sleep 8)
                             {:type :info :f fault :value :start}
                             (gen/sleep 15)
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
