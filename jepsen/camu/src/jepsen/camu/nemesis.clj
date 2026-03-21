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
  "Restarts the camu process on the current node."
  []
  (c/exec :bash :-c
          "nohup /opt/camu/camu --config /etc/camu/camu.yaml >> /var/log/camu.log 2>&1 &"))

(defn pause-camu!
  "Sends SIGSTOP to camu on the current node."
  []
  (c/exec :pkill :-STOP :-f "camu" (c/lit "|| true")))

(defn resume-camu!
  "Sends SIGCONT to camu on the current node."
  []
  (c/exec :pkill :-CONT :-f "camu" (c/lit "|| true")))

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
  "A nemesis that kills and restarts camu processes."
  []
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node]
     (c/on-nodes test [node] (fn [_ _] (start-camu!)))
     [:restarted node])
   (fn stop [test node]
     (c/on-nodes test [node] (fn [_ _] (kill-camu!)))
     [:killed node])))

(defn pause-nemesis
  "A nemesis that pauses and resumes camu processes."
  []
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node]
     (c/on-nodes test [node] (fn [_ _] (resume-camu!)))
     [:resumed node])
   (fn stop [test node]
     (c/on-nodes test [node] (fn [_ _] (pause-camu!)))
     [:paused node])))

(defn rejoin-nemesis
  "A nemesis that kills a node, waits for lease expiry, then restarts it.
   This specifically tests the epoch fencing mechanism: the restarted instance
   must acquire a new epoch before writing."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (case (:f op)
        :rejoin-start
        (let [node (rand-nth (:nodes test))]
          (info "Rejoin nemesis: killing" node "for lease expiry test")
          (c/on-nodes test [node] (fn [_ _] (kill-camu!)))
          ;; Wait longer than the lease TTL so the lease is reclaimed
          (Thread/sleep 20000)
          (c/on-nodes test [node] (fn [_ _] (start-camu!)))
          (assoc op :type :info :value [:rejoined node]))
        :rejoin-stop
        (assoc op :type :info :value :no-op)))
    (teardown! [this test])))

(defn s3-partition-nemesis
  "A nemesis that blocks/unblocks a random node's access to MinIO port 9000.
   Tests what happens when a camu instance can't reach S3 but still receives
   client requests."
  [s3-host]
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node]
     (c/on-nodes test [node] (fn [_ _] (unblock-s3! s3-host)))
     [:s3-unblocked node])
   (fn stop [test node]
     (c/on-nodes test [node] (fn [_ _] (block-s3! s3-host)))
     [:s3-blocked node])))

(defn clock-skew-nemesis
  "A nemesis that introduces clock drift on nodes.
   Tests lease expiry under clock drift and epoch fencing correctness."
  []
  (nemesis/clock-scrambler 10))

(defn composed-nemesis
  "Returns a nemesis that composes fault types specified in the faults set.
   Supported fault keys: :kill :partition :pause :rejoin :s3-partition :clock-skew"
  ([] (composed-nemesis #{:kill :partition :pause}))
  ([faults]
   (nemesis/compose
    (cond-> {}
      (:kill faults)
      (assoc {:kill :start} (kill-nemesis)
             {:kill :stop}  (kill-nemesis))

      (:partition faults)
      (assoc {:partition :start} (nemesis/partitioner nemesis/bridge)
             {:partition :stop}  (nemesis/partitioner nemesis/bridge))

      (:pause faults)
      (assoc {:pause :start} (pause-nemesis)
             {:pause :stop}  (pause-nemesis))

      (:rejoin faults)
      (assoc {:rejoin :start} (rejoin-nemesis)
             {:rejoin :stop}  (rejoin-nemesis))

      (:s3-partition faults)
      (assoc {:s3-partition :start} (s3-partition-nemesis "minio")
             {:s3-partition :stop}  (s3-partition-nemesis "minio"))

      (:clock-skew faults)
      (assoc {:clock-skew :start} (clock-skew-nemesis)
             {:clock-skew :stop}  (clock-skew-nemesis))))))

(defn fault-cycles
  "Returns a sequence of gen/cycle generators for the given fault keys."
  [faults]
  (for [fault faults]
    (gen/cycle
     [(gen/sleep 5)
      {:type :info :f fault :value :start}
      (gen/sleep 10)
      {:type :info :f fault :value :stop}])))

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
