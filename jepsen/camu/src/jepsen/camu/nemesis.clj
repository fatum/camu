(ns jepsen.camu.nemesis
  (:require [clojure.java.shell :as sh]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [nemesis :as nemesis]
                    [generator :as gen]
                    [control :as c]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.combined :as nc]
            [jepsen.camu.client :as client]
            [jepsen.camu.db :as db]
            [cheshire.core :as json]))

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

(defn wipe-camu-local-state!
  "Deletes camu's local data directory (active segments, cache, epoch sidecar)
   so a restarted node must rebuild its view entirely from S3."
  []
  (c/exec :bash :-lc
          (str "rm -rf " db/camu-data "; mkdir -p " db/camu-data "/cache")))

(defn plant-failed-s3-state!
  "Writes failed/orphaned state directly into the S3 bucket that a redeployed
   node must tolerate and that the cluster's GC must clean up: an orphaned
   segment object under the test topic (no metadata/index references it), a
   stale instance registration for a phantom node, and a stale ISR entry naming
   a dead leader. A redeployed node joining with this existing+failed bucket
   state must never serve the orphaned segment or be confused by the stale
   coordination objects; the object-store GC must reclaim them."
  [test]
  (let [topic (:topic test)
        mc-fn (fn [& args]
                (sh/sh "mc" "--config-dir" "/tmp/.mc-jepsen" "alias" "set"
                       "local" "http://minio:9000" "minioadmin" "minioadmin")
                (apply sh/sh (concat ["mc" "--config-dir" "/tmp/.mc-jepsen"] args)))]
    ;; Orphaned segment object under the topic's data prefix, referenced by no
    ;; .meta.json, so reads must never surface it.
    (let [key (str "local/camu-data/segments/" topic "/0/9007199254740991.log")]
      (mc-fn "pipe" key :in "orphaned segment data that must never be served"))
    ;; Stale instance registration for a node that no longer exists.
    (let [key (str "local/camu-data/_coordination/instances/phantom-node.json")]
      (mc-fn "pipe" key :in
             (json/generate-string
              {:instance_id "phantom-node"
               :address "10.0.0.99:8080"
               :internal_address "10.0.0.99:8081"
               :heartbeat_at "2000-01-01T00:00:00Z"})))
    ;; Stale ISR entry naming a dead leader at a lower epoch; the real leader's
    ;; epoch-guarded update must overwrite it.
    (let [key (str "local/camu-data/_coordination/isr/" topic "/0.json")]
      (mc-fn "pipe" key :in
             (json/generate-string
              {:partition 0
               :isr ["phantom-node"]
               :leader "phantom-node"
               :leader_epoch 0
               :high_watermark 0
               :updated_at "2000-01-01T00:00:00Z"})))))

(defn restart-wipe-nemesis
  "A nemesis that kills a node and wipes its local data directory before
   restarting it. This simulates a node whose local disk state is lost or a
   full re-deploy on the same host: the restarted node must recover all topics,
   assignments, ISR, and committed segments from S3 alone, and must not lose or
   duplicate acknowledged writes across the restart. Unlike kill-nemesis (which
   restarts with local state intact), this exercises the S3-only recovery path
   that a redeployed node goes through."
  []
  (let [wiped (atom #{})]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (case (:value op)
          :start (let [node (rand-nth (:nodes test))]
                   (info "Restart-wipe nemesis: killing and wiping" node)
                   (c/on-nodes test [node] (fn [_ _] (kill-camu!)))
                   ;; Give the killed process time to fully die before wiping.
                   (Thread/sleep 2000)
                   (c/on-nodes test [node] (fn [_ _] (wipe-camu-local-state!)))
                   (swap! wiped conj node)
                   (assoc op :value [:wiped node]))
          :stop  (let [to-restart (vec @wiped)]
                   (reset! wiped #{})
                   (when (seq to-restart)
                     (Thread/sleep 3000)
                     (doseq [node to-restart]
                       (try
                         (c/on-nodes test [node] (fn [_ _] (start-camu!)))
                         (catch Exception e
                           (info "Failed to restart" node (.getMessage e))))))
                   (assoc op :value [:restarted to-restart]))))
      (teardown! [this test]
        (let [to-restart (vec @wiped)]
          (when (seq to-restart)
            (Thread/sleep 3000)
            (doseq [node to-restart]
              (try
                (c/on-nodes test [node] (fn [_ _] (start-camu!)))
                (catch Exception _)))))))))

(defn redeploy-nemesis
  "A nemesis that re-deploys a node against S3 state that already exists AND
   contains failed/orphaned objects. The node is stopped, its local state is
   wiped, and failed state is planted directly in the bucket (an orphaned
   segment, a phantom instance registration, and a stale ISR). The node then
   restarts under its same identity and must adopt the existing topic, recover
   committed segments, tolerate the failed objects (never serving the orphan or
   trusting the stale coordination), and let the object-store GC reclaim them —
   all without losing acknowledged writes or serving ghosts."
  []
  (let [redeployed (atom #{})]
    (reify nemesis/Nemesis
      (setup! [this test] this)
      (invoke! [this test op]
        (case (:value op)
          :start (let [node (rand-nth (:nodes test))]
                   (info "Redeploy nemesis: stopping" node "and planting failed S3 state")
                   (c/on-nodes test [node] (fn [_ _] (stop-camu!)))
                   (Thread/sleep 2000)
                   (c/on-nodes test [node]
                               (fn [_ _]
                                 (wipe-camu-local-state!)))
                   ;; Plant failed state directly into the bucket from the
                   ;; control node (mc is available there).
                   (plant-failed-s3-state! test)
                   (swap! redeployed conj node)
                   (assoc op :value [:redeployed node]))
          :stop  (let [to-restart (vec @redeployed)]
                   (doseq [node to-restart]
                     (try
                       (c/on-nodes test [node] (fn [_ _] (start-camu!)))
                       (catch Exception e
                         (info "Failed to redeploy" node (.getMessage e)))))
                   (reset! redeployed #{})
                   (assoc op :value [:redeployed-restarted to-restart]))))
      (teardown! [this test]
        (let [to-restart (vec @redeployed)]
          (when (seq to-restart)
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

(defn s3-unavailable-nemesis
  "A nemesis that blocks EVERY node's access to MinIO port 9000, simulating a
   full object-store outage rather than a single-node partition. All camu
   nodes lose S3 simultaneously; produces must pause (diskless flush/commit
   cannot reach S3) instead of dropping data, and after S3 returns every
   acknowledged record must be durable and gapless."
  [s3-host]
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:value op)
        :start (do (c/on-nodes test (:nodes test) (fn [_ _] (block-s3! s3-host)))
                   (assoc op :value :s3-unavailable))
        :stop  (do (c/on-nodes test (:nodes test) (fn [_ _] (unblock-s3! s3-host)))
                   (assoc op :value :s3-available))))
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
                         :s3-partition :s3-unavailable :clock-skew :leader-kill
                         :partition-ring :leader-pause-then-ack

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

      (:restart-wipe faults)
      (assoc #{:restart-wipe} (restart-wipe-nemesis))

      (:redeploy faults)
      (assoc #{:redeploy} (redeploy-nemesis))

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

      (:s3-unavailable faults)
      (assoc #{:s3-unavailable} (s3-unavailable-nemesis "minio"))

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
    :restart-wipe (gen/cycle
                   [(gen/sleep 8)
                    {:type :info :f fault :value :start}
                    (gen/sleep 15)
                    {:type :info :f fault :value :stop}])
    :redeploy (gen/cycle
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
