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
          "nohup /opt/camu/camu serve --config /etc/camu/camu.yaml >> /var/log/camu.log 2>&1 &"))

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
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (let [node (rand-nth (:nodes test))]
        (case (:value op)
          :start (do (c/on-nodes test [node] (fn [_ _] (kill-camu!)))
                     (assoc op :value [:killed node]))
          :stop  (do (c/on-nodes test (:nodes test) (fn [_ _] (start-camu!)))
                     (assoc op :value :restarted)))))
    (teardown! [this test]
      (c/on-nodes test (:nodes test) (fn [_ _] (start-camu!))))))

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
   Supported fault keys: :kill :partition :pause :rejoin :s3-partition :clock-skew

   For :kill — start = kill process, stop = restart process
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

      (:s3-partition faults)
      (assoc #{:s3-partition} (s3-partition-nemesis "minio"))

      (:clock-skew faults)
      (assoc #{:clock-skew} (clock-skew-nemesis))))))

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
