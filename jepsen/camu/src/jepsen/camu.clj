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
            [jepsen.camu.kafka-client :as kafka-client]
            [jepsen.camu.nemesis :as nem]
            [jepsen.camu.checker :as camu-checker]
            [jepsen.camu.db :as db])
  (:import (java.util UUID)))

(def default-partitions 4)
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
  [partitions offsets]
  (fn [_ _]
    (let [p (rand-int partitions)]
      {:type  :invoke
       :f     :consume
       :value {:partition p
               :offset    (get @offsets p 0)}})))

(defn mixed-workload-gen
  "Returns a mixed generator: ~70% produce, ~30% consume."
  [partitions counter offsets]
  (gen/mix [(produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (consume-gen partitions offsets)
            (consume-gen partitions offsets)
            (consume-gen partitions offsets)]))

(defn large-requests-workload-gen
  "Returns a mixed generator with concurrent 5 KB produces and consumes."
  [partitions counter offsets]
  (gen/mix [(large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (large-produce-gen counter)
            (consume-gen partitions offsets)
            (consume-gen partitions offsets)
            (consume-gen partitions offsets)]))

(defn replica-flushed-reads-workload-gen
  "Returns a produce-heavy workload intended for replica reads against flushed
   data. Pair it with read-mode :replica and a graceful leave-style fault."
  [partitions counter offsets]
  (gen/mix [(produce-gen counter)
           (produce-gen counter)
           (produce-gen counter)
           (produce-gen counter)
           (produce-gen counter)
           (produce-gen counter)
           (produce-gen counter)
           (produce-gen counter)
           (consume-gen partitions offsets)
           (consume-gen partitions offsets)]))

(defn concurrent-writes-gen
  "Generator that produces messages targeting a single partition to create
   write contention. All produces go to partition 0."
  [counter]
  (fn [_ _]
    (let [n (swap! counter inc)]
      {:type  :invoke
       :f     :produce
       :value {:key   (str "k-" n)
               :value (str "v-" n)
               :partition 0}})))

(defn concurrent-writes-workload-gen
  "Produce-heavy workload where all clients write to the same partition.
   Stresses concurrent writes, leader serialization, and offset assignment."
  [partitions counter offsets]
  (gen/mix [(concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)
           (concurrent-writes-gen counter)]))

(defn commit-offsets-gen
  "Requests a commit for a random partition using the client-local consumed
   offset tracked by the Jepsen client."
  [partitions]
  (fn [_ _]
    {:type  :invoke
     :f     :commit-offsets
     :value {:partition (rand-int partitions)}}))

(defn get-offsets-gen
  "Fetches the committed offsets for the current Jepsen client consumer ID."
  []
  (fn [_ _]
    {:type  :invoke
     :f     :get-offsets
     :value {}}))

(defn offsets-workload-gen
  "Mixed workload for explicit consumer offset semantics."
  [partitions counter offsets]
  (gen/mix [(produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (consume-gen partitions offsets)
            (consume-gen partitions offsets)
            (consume-gen partitions offsets)
            (commit-offsets-gen partitions)
            (commit-offsets-gen partitions)
            (get-offsets-gen)]))

(defn sql-count-gen
  "Returns a generator emitting :sql-query ops that ask for the current row
   count in the topic. The checker ignores these (it only consumes the final
   drain query) but they exercise the SQL path during the fault window."
  []
  (fn [_ _]
    {:type  :invoke
     :f     :sql-query
     :value {:sql   "select count(*)::BIGINT as n from jepsen_topic"
             :final false}}))

(defn retention-state-gen
  "Continuously samples source-segment and export-checkpoint state during the
   short-retention lifecycle workload."
  []
  (fn [_ _]
    {:type :invoke :f :retention-state :value {}}))

(defn sql-drain-query-gen
  "Returns a generator that issues ONE final drain-style SQL query. The
   checker consumes :ok :sql-query ops with :final true."
  [topic typed?]
  (fn [_ _]
    {:type  :invoke
     :f     :sql-query
     :value {:sql   (if typed?
                      (str "select key, id, paid from \"" topic "\"")
                      (str "select key from \"" topic "\""))
             :final true}}))

(declare retention-lifecycle?)

(defn typed-value
  [n]
  (str "{\"id\":" n ",\"paid\":true}"))

(defn sql-workload-gen
  "Produce-heavy workload that mixes in periodic :sql-query probes during
   the fault window. The actual end-of-run SQL drain is appended as a
   distinct phase in camu-test so it runs AFTER the nemesis stops and the
   exporter has had time to catch up."
  [retention-lifecycle? typed? _partitions counter _offsets]
  (let [producer (if typed?
                   (produce-gen counter typed-value)
                   (produce-gen counter))]
    (gen/mix (cond-> [producer producer producer producer producer producer producer producer
                      (sql-count-gen)]
               retention-lifecycle? (into (repeat 3 (retention-state-gen)))))))

(defn num-partitions
  [opts]
  (get opts :num-partitions default-partitions))

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
  (let [partitions (num-partitions opts)]
    (case (or (:workload opts) :mixed)
      :idempotent (gen/mix [(idempotent-produce-gen counter)
                            (idempotent-produce-gen counter)
                            (idempotent-produce-gen counter)
                            (idempotent-produce-gen counter)
                            (idempotent-produce-gen counter)
                            (idempotent-produce-gen counter)
                            (idempotent-produce-gen counter)
                            (consume-gen partitions offsets)
                            (consume-gen partitions offsets)
                            (consume-gen partitions offsets)])
      :offsets (offsets-workload-gen partitions counter offsets)
      :large-requests (large-requests-workload-gen partitions counter offsets)
      :replica-flushed-reads (replica-flushed-reads-workload-gen partitions counter offsets)
      :concurrent-writes (concurrent-writes-workload-gen partitions counter offsets)
      :sql (sql-workload-gen (retention-lifecycle? opts) (:typed opts) partitions counter offsets)
      (mixed-workload-gen partitions counter offsets))))

(defn drain-gen
  "Returns a generator that drains all partitions using the given operation.
   Defaults to :drain (leader reads)."
  ([partitions] (drain-gen partitions :drain))
  ([partitions op]
   (map (fn [p]
          {:type  :invoke
           :f     op
           :value {:partition p :offset 0}})
        (range partitions))))

(defn replicated?
  "Returns true if the test opts request replication (RF > 1)."
  [opts]
  (> (get opts :replication-factor 1) 1))

(defn api-mode
  [opts]
  (or (:api opts) :http))

(defn http-api?
  [opts]
  (= :http (api-mode opts)))

(defn kafka-api?
  [opts]
  (= :kafka (api-mode opts)))

(defn supported-workload?
  [opts]
  (or (http-api? opts)
      (contains? #{:mixed :large-requests :concurrent-writes :idempotent}
                 (or (:workload opts) :mixed))))

(defn sql-workload?
  [opts]
  (= :sql (:workload opts)))

(defn retention-lifecycle?
  "True only for the explicit short-retention SQL lifecycle scenario."
  [opts]
  (true? (:retention-lifecycle opts)))

(defn sql-checkers
  "The SQL checker set. Retention assertions belong only to the explicit
   short-retention lifecycle run, never to ordinary SQL failover coverage."
  [opts]
  (cond-> (cond-> {:sql-visibility (camu-checker/sql-visibility-checker)}
            (:typed opts) (assoc :typed-sql (camu-checker/typed-sql-checker)))
    (retention-lifecycle? opts)
    (assoc :retention-export (camu-checker/retention-export-checker))))

(defn validate-opts!
  [opts]
  (when-not (supported-workload? opts)
    (throw (ex-info "Kafka Jepsen path currently supports only mixed, large-requests, concurrent-writes, and idempotent workloads"
                    {:type :unsupported-workload
                     :api (api-mode opts)
                     :workload (:workload opts)})))
  (when (and (kafka-api? opts)
             (not= :leader (or (:read-mode opts) :leader)))
    (throw (ex-info "Kafka Jepsen path only supports leader read-mode"
                    {:type :unsupported-read-mode
                     :api :kafka
                     :read-mode (:read-mode opts)})))
  (when (and (sql-workload? opts) (kafka-api? opts))
    (throw (ex-info "SQL workload requires --api http"
                    {:type :unsupported-workload
                     :api :kafka
                     :workload :sql})))
  opts)

(defn checker-suite
  "Returns the appropriate checker composition based on whether the test
   is running in replicated mode.

   Epoch-fencing coverage gap: single-leader-checker groups produces by
   (partition, leader-epoch) and verifies one node per epoch. The HTTP
   client captures :leader-epoch and :node from response headers; the
   Kafka client's RecordMetadata does not expose leader epoch. Therefore
   single-leader-checker is HTTP-only. For the Kafka replicated path we
   fall back to no-split-brain-checker, which detects conflicting values
   at the same (partition, offset) from drain data — a weaker but still
   meaningful split-brain signal."
  [opts]
  (let [meta-checkers (cond-> {:stats         (checker/stats)
                               :availability  (camu-checker/availability-checker)
                               :recovery-time (camu-checker/recovery-time-checker)
                               :ex            (camu-checker/unhandled-exception-checker)}
                        (db/capture-node-logs?)
                        (assoc :server-log
                               (checker/log-file-pattern
                                #"(?i)(panic:|fatal error:|segmentation fault|sigsegv|sigabrt|warning: data race)"
                                "camu.log")))
        base (merge meta-checkers
                    {:leader-drain-coverage (camu-checker/drain-coverage-checker :drain)})
        replicated-base (if (and (replicated? opts) (http-api? opts))
                          (assoc base :replica-drain-coverage
                                 (camu-checker/drain-coverage-checker :replica-drain))
                          base)]
    (checker/compose
     (cond
       (= :idempotent (:workload opts))
       (merge base
              {:exactly-once        (camu-checker/exactly-once-checker)
               :offset-monotonicity (camu-checker/offset-monotonicity-checker)
               :total-order         (camu-checker/total-order-checker)})

       (= :offsets (:workload opts))
       (merge replicated-base
              {:committed-durability (camu-checker/committed-durability-checker)
               :offset-monotonicity  (camu-checker/offset-monotonicity-checker)
               :total-order          (camu-checker/total-order-checker)
               :consumer-offsets     (camu-checker/consumer-offset-checker)})

       (sql-workload? opts)
       ;; SQL workloads verify their final Parquet view. Only the dedicated
       ;; short-retention lifecycle run makes source-deletion assertions.
       (merge meta-checkers (sql-checkers opts))

       (replicated? opts)
       (cond-> (merge replicated-base
                      {:committed-durability (camu-checker/committed-durability-checker)
                       :truncation-safety    (camu-checker/truncation-safety-checker)
                       :offset-monotonicity  (camu-checker/offset-monotonicity-checker)
                       :total-order          (camu-checker/total-order-checker)
                       :no-split-brain       (camu-checker/no-split-brain-checker)})
         (http-api? opts)
         (assoc :no-ghost-reads (camu-checker/no-ghost-reads-checker)
                :single-leader (camu-checker/single-leader-checker)
                :hw-monotonicity (camu-checker/hw-monotonicity-checker)
                :hw-global-monotonicity (camu-checker/hw-global-monotonicity-checker)
                :read-your-writes (camu-checker/read-your-writes-checker)
                :replica-convergence (camu-checker/replica-convergence-checker)))

       :else
       (cond-> (merge base
                      {:no-data-loss        (camu-checker/no-data-loss-checker)
                       :offset-monotonicity (camu-checker/offset-monotonicity-checker)
                       :no-split-brain      (camu-checker/no-split-brain-checker)
                       :total-order         (camu-checker/total-order-checker)})
         (http-api? opts)
         (assoc :read-your-writes (camu-checker/read-your-writes-checker)))))))

(defn camu-test
  "Constructs a Jepsen test map for camu."
  [opts]
  (let [opts            (validate-opts! opts)
        faults         (:faults opts #{:kill})
        partitions     (num-partitions opts)
        topic           (str "jepsen-test-" (str (UUID/randomUUID)))
        counter         (atom 0)
        consume-offsets (atom {})]
    (merge tests/noop-test
           opts
           {:name            (str "camu-" (name (api-mode opts)))
            :topic           topic
            :num-partitions  partitions
            :read-mode       (or (:read-mode opts) :leader)
            :enable-sql?     (sql-workload? opts)
            :os              os/noop
            :db              (db/db)
            :client          (if (kafka-api? opts)
                               (kafka-client/client)
                               (client/client))
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
               ]
              (when-not (sql-workload? opts)
                [(gen/log "Draining all partitions for verification...")
                 (gen/clients (drain-gen partitions))])
              ;; Phase 4: drain ALL partitions from replicas to verify convergence
              (when (and (not (sql-workload? opts))
                         (replicated? opts)
                         (http-api? opts))
                [(gen/log "Draining all partitions from replicas...")
                 (gen/clients (drain-gen partitions :replica-drain))])
              ;; Phase 5 (sql workload only): sleep to let the async
              ;; parquet exporter catch up on any just-flushed segments,
              ;; then issue a single final drain SQL query that the
              ;; sql-visibility checker consumes. Pinned to process 0 so
              ;; exactly one op fires regardless of client count.
              (when (sql-workload? opts)
                (concat
                 [(gen/log "Waiting 15s for async parquet exporter to catch up...")
                  (gen/sleep 15)
                  (gen/log "Issuing final SQL drain query...")
                  (gen/on-threads
                   #{0}
                   (gen/once (sql-drain-query-gen topic (:typed opts))))]
                 (when (retention-lifecycle? opts)
                   [(gen/log "Verifying checkpoint-covered source retention...")
                    (gen/on-threads
                     #{0}
                     (gen/once {:type :invoke :f :retention-state :value {}}))])))))})))

(def cli-opts
  "Additional CLI options for camu tests."
  [[nil "--s3-endpoint URL" "MinIO/S3 endpoint"
    :default "http://minio:9000"]
   [nil "--camu-binary PATH" "Path to camu binary"
    :default "/jepsen/camu/camu"]
   [nil "--http-port PORT" "HTTP port for camu"
    :default 8080
    :parse-fn #(Integer/parseInt %)]
   [nil "--kafka-port PORT" "Kafka port for camu"
    :default 9092
    :parse-fn #(Integer/parseInt %)]
   [nil "--api NAME" "Client API: http or kafka"
    :default :http
    :parse-fn keyword
    :validate [#{:http :kafka} "must be one of: http, kafka"]]
   [nil "--num-partitions N" "Number of partitions in the Jepsen topic"
    :default default-partitions
    :parse-fn #(Integer/parseInt %)]
   [nil "--faults FAULTS" "Comma-separated fault types: kill,partition,partition-ring,pause,leader-kill"
    :default #{:kill}
    :parse-fn (fn [s] (set (map keyword (clojure.string/split s #","))))]
   [nil "--replication-factor N" "Topic replication factor"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   [nil "--min-insync-replicas N" "Minimum in-sync replicas for ack"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   [nil "--segment-max-size BYTES" "Segment flush size threshold in bytes"
    :default 104857600
    :parse-fn #(Long/parseLong %)]
   [nil "--segment-max-age DURATION" "Segment flush age threshold, e.g. 30s"
    :default "1m"]
   [nil "--topic-retention DURATION" "TEST-ONLY topic retention for lifecycle scenarios"
    :default "24h"]
   [nil "--retention-lifecycle BOOL" "Enable source-retention assertions for the SQL lifecycle scenario"
    :default false
    :parse-fn #(Boolean/parseBoolean %)]
   [nil "--typed BOOL" "Use an immutable JSON typed schema for SQL workload"
    :default false
    :parse-fn #(Boolean/parseBoolean %)]
   [nil "--read-mode MODE" "Read routing mode: leader, replica, or any"
    :default :leader
    :parse-fn keyword
    :validate [#{:leader :replica :any} "must be one of: leader, replica, any"]]
   [nil "--workload NAME" "Workload: mixed, large-requests, replica-flushed-reads, idempotent, offsets, concurrent-writes, or sql"
    :default :mixed
    :parse-fn keyword
    :validate [#{:mixed :large-requests :replica-flushed-reads :idempotent :offsets :concurrent-writes :sql} "must be one of: mixed, large-requests, replica-flushed-reads, idempotent, offsets, concurrent-writes, sql"]]])

(defn -main
  "Entry point for the Jepsen CLI."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn  camu-test
                                 :opt-spec cli-opts})
          (cli/serve-cmd))
   args))
