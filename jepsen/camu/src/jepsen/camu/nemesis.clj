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

(defn composed-nemesis
  "Returns a nemesis that composes kill, partition, and pause faults."
  []
  (nemesis/compose
   {{:kill :start}      (kill-nemesis)
    {:partition :start}  (nemesis/partitioner nemesis/bridge)
    {:pause :start}      (pause-nemesis)
    {:kill :stop}        (kill-nemesis)
    {:partition :stop}   (nemesis/partitioner nemesis/bridge)
    {:pause :stop}       (pause-nemesis)}))

(defn nemesis-generator
  "Returns a generator that injects faults with ~5s stagger."
  [time-limit]
  (gen/phases
   ;; Active phase: interleave faults
   (->> (gen/mix [(gen/cycle
                   [(gen/sleep 5)
                    {:type :info :f :kill :value :start}
                    (gen/sleep 10)
                    {:type :info :f :kill :value :stop}])
                  (gen/cycle
                   [(gen/sleep 5)
                    {:type :info :f :partition :value :start}
                    (gen/sleep 10)
                    {:type :info :f :partition :value :stop}])
                  (gen/cycle
                   [(gen/sleep 5)
                    {:type :info :f :pause :value :start}
                    (gen/sleep 10)
                    {:type :info :f :pause :value :stop}])])
        (gen/stagger 5)
        (gen/time-limit time-limit))
   ;; Recovery: stop all faults
   (gen/nemesis
    (gen/once {:type :info :f :kill :value :stop}))
   (gen/nemesis
    (gen/once {:type :info :f :partition :value :stop}))
   (gen/nemesis
    (gen/once {:type :info :f :pause :value :stop}))
   ;; Let the cluster stabilize
   (gen/sleep 15)))
