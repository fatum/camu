(ns jepsen.camu
  (:gen-class)
  (:require [clojure.string]
            [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.camu.client :as client]
            [jepsen.camu.nemesis :as nem]
            [jepsen.camu.checker :as camu-checker]
            [jepsen.camu.db :as db])
  (:import (java.util UUID)))

(def partitions 4)
(def large-value-size-bytes (* 5 1024))

(defn fixed-size-value
  "Builds an ASCII string with exactly size-bytes characters."
  [prefix n size-bytes]
  (let [base       (str prefix "-" n "-")
        filler-len (max 0 (- size-bytes (count base)))]
    (str base (apply str (repeat filler-len \x)))))

(defn produce-gen
  "Returns a generator that produces messages with unique sequential keys."
  ([counter]
   (produce-gen counter (fn [n] (str "v-" n))))
  ([counter value-fn]
  (fn [_ _]
    (let [n (swap! counter inc)]
      {:type  :invoke
       :f     :produce
       :value {:key   (str "k-" n)
               :value (value-fn n)}}))))

(defn large-produce-gen
  "Returns a generator that produces 5 KB values."
  [counter]
  (produce-gen counter #(fixed-size-value "large-v" % large-value-size-bytes)))

(defn consume-gen
  "Returns a generator that consumes from a random partition, tracking
   the highest offset seen per partition so reads advance forward."
  [offsets]
  (fn [_ _]
    (let [p (rand-int partitions)]
      {:type  :invoke
       :f     :consume
       :value {:partition p
               :offset    (get @offsets p 0)}})))

(defn mixed-workload-gen
  "Returns a mixed generator: ~70% produce, ~30% consume."
  [counter offsets]
  (gen/mix [(produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (consume-gen offsets)
            (consume-gen offsets)
            (consume-gen offsets)]))

(defn large-requests-workload-gen
  "Returns a mixed generator with concurrent 5 KB produces and consumes."
  [counter offsets]
  (gen/mix [(large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (consume-gen offsets)
            (consume-gen offsets)
            (consume-gen offsets)]))

(defn replica-flushed-reads-workload-gen
  "Returns a produce-heavy workload intended for replica reads against flushed
   data. Pair it with read-mode :replica and a graceful leave-style fault."
  [counter offsets]
  (gen/mix [(produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (consume-gen offsets)
            (consume-gen offsets)]))

(defn idempotent-produce-gen
  "Generator for idempotent produce operations."
  [counter]
  (fn [_ _]
    (let [n (swap! counter inc)]
      {:type  :invoke
       :f     :idempotent-produce
       :value {:key   (str "k-" n)
               :value (str "v-" n)}})))

(defn workload-gen
  [opts counter offsets]
  (case (or (:workload opts) :mixed)
    :idempotent (gen/mix [(idempotent-produce-gen counter)
                          (idempotent-produce-gen counter)
                          (idempotent-produce-gen counter)
                          (idempotent-produce-gen counter)
                          (idempotent-produce-gen counter)
                          (idempotent-produce-gen counter)
                          (idempotent-produce-gen counter)
                          (consume-gen offsets)
                          (consume-gen offsets)
                          (consume-gen offsets)])
    :large-requests (large-requests-workload-gen counter offsets)
    :replica-flushed-reads (replica-flushed-reads-workload-gen counter offsets)
    (mixed-workload-gen counter offsets)))

(defn drain-gen
  "Returns a generator that drains all partitions using the given operation.
   Defaults to :drain (leader reads)."
  ([] (drain-gen :drain))
  ([op]
   (map (fn [p]
          {:type  :invoke
           :f     op
           :value {:partition p :offset 0}})
        (range partitions))))

(defn replicated?
  "Returns true if the test opts request replication (RF > 1)."
  [opts]
  (> (get opts :replication-factor 1) 1))

(defn checker-suite
  "Returns the appropriate checker composition based on whether the test
   is running in replicated mode."
  [opts]
  (if (= :idempotent (:workload opts))
    (checker/compose
     {:exactly-once       (camu-checker/exactly-once-checker)
      :offset-monotonicity (camu-checker/offset-monotonicity-checker)
      :total-order        (camu-checker/total-order-checker)
      :availability       (camu-checker/availability-checker)
      :recovery-time      (camu-checker/recovery-time-checker)
      :stats              (checker/stats)})
    (if (replicated? opts)
    ;; Replicated: use key-based durability and epoch-based leader checks
    (checker/compose
     {:committed-durability  (camu-checker/committed-durability-checker)
      :no-ghost-reads        (camu-checker/no-ghost-reads-checker)
      :single-leader         (camu-checker/single-leader-checker)
      :hw-monotonicity       (camu-checker/hw-monotonicity-checker)
      :truncation-safety     (camu-checker/truncation-safety-checker)
      :offset-monotonicity   (camu-checker/offset-monotonicity-checker)
      :total-order           (camu-checker/total-order-checker)
      :replica-convergence   (camu-checker/replica-convergence-checker)
      :availability          (camu-checker/availability-checker)
      :recovery-time         (camu-checker/recovery-time-checker)
      :stats                 (checker/stats)})
    ;; Unreplicated: original checkers
    (checker/compose
     {:no-data-loss        (camu-checker/no-data-loss-checker)
      :offset-monotonicity (camu-checker/offset-monotonicity-checker)
      :no-split-brain      (camu-checker/no-split-brain-checker)
      :total-order         (camu-checker/total-order-checker)
      :availability        (camu-checker/availability-checker)
      :lease-fencing       (camu-checker/lease-fencing-checker)
      :recovery-time       (camu-checker/recovery-time-checker)
      :stats               (checker/stats)}))))

(defn camu-test
  "Constructs a Jepsen test map for camu."
  [opts]
  (let [faults  (:faults opts #{:kill})
        topic           (str "jepsen-test-" (str (UUID/randomUUID)))
        counter         (atom 0)
        consume-offsets (atom {})]
    (merge tests/noop-test
           opts
           {:name            "camu"
            :topic           topic
            :read-mode       (or (:read-mode opts) :leader)
            :os              os/noop
            :db              (db/db)
            :client          (client/client)
            :consume-offsets consume-offsets
            :nemesis   (nem/composed-nemesis faults)
            :checker   (checker-suite opts)
            :generator
            (apply gen/phases
             (concat
              [;; Phase 1: clients produce+consume while nemesis injects faults
               (gen/time-limit
                (:time-limit opts 300)
                (gen/nemesis
                 (->> (gen/mix (nem/fault-cycles faults))
                      (gen/stagger 5))
                 (gen/clients
                  (->> (workload-gen opts counter consume-offsets)
                       (gen/stagger 1/10)))))
               ;; Phase 2: stop all active faults, restart nodes
               (gen/log "Stopping nemesis, restarting all nodes...")
               (apply gen/phases
                      (for [fault faults]
                        (gen/nemesis (gen/once {:type :info :f fault :value :stop}))))
               (gen/log "Recovering — waiting 15s for cluster stabilization...")
               (gen/sleep 15)
               ;; Phase 3: drain ALL partitions from leader
               (gen/log "Draining all partitions for verification...")
               (gen/clients (drain-gen))]
              ;; Phase 4: drain ALL partitions from replicas to verify convergence
              (when (replicated? opts)
                [(gen/log "Draining all partitions from replicas...")
                 (gen/clients (drain-gen :replica-drain))])))})))

(def cli-opts
  "Additional CLI options for camu tests."
  [[nil "--s3-endpoint URL" "MinIO/S3 endpoint"
    :default "http://minio:9000"]
   [nil "--camu-binary PATH" "Path to camu binary"
    :default "/jepsen/camu/camu"]
   [nil "--http-port PORT" "HTTP port for camu"
    :default 8080
    :parse-fn #(Integer/parseInt %)]
   [nil "--faults FAULTS" "Comma-separated fault types: kill,partition,pause,leader-kill"
    :default #{:kill}
    :parse-fn (fn [s] (set (map keyword (clojure.string/split s #","))))]
   [nil "--replication-factor N" "Topic replication factor"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   [nil "--min-insync-replicas N" "Minimum in-sync replicas for ack"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   [nil "--read-mode MODE" "Read routing mode: leader, replica, or any"
    :default :leader
    :parse-fn keyword
    :validate [#{:leader :replica :any} "must be one of: leader, replica, any"]]
   [nil "--workload NAME" "Workload: mixed, large-requests, replica-flushed-reads, or idempotent"
    :default :mixed
    :parse-fn keyword
    :validate [#{:mixed :large-requests :replica-flushed-reads :idempotent} "must be one of: mixed, large-requests, replica-flushed-reads, idempotent"]]])

(defn -main
  "Entry point for the Jepsen CLI."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn  camu-test
                                 :opt-spec cli-opts})
          (cli/serve-cmd))
   args))
