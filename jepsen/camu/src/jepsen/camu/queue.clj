(ns jepsen.camu.queue
  "Entry point for running Elle queue consistency tests against Camu's Kafka
  protocol. Constructs a Jepsen test using the queue workload ported from the
  Redpanda test suite."
  (:gen-class)
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [history :as h]
                    [nemesis :as nemesis]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.camu [db :as db]
                         [nemesis :as nem]]
            [jepsen.camu.workload [queue :as queue]]))

(def logging-overrides
  "Quiet down noisy Kafka client loggers."
  {"org.apache.kafka.clients.FetchSessionHandler"                    :warn
   "org.apache.kafka.clients.Metadata"                               :off
   "org.apache.kafka.clients.NetworkClient"                          :error
   "org.apache.kafka.clients.admin.AdminClientConfig"                :warn
   "org.apache.kafka.clients.admin.KafkaAdminClient"                 :warn
   "org.apache.kafka.clients.admin.internals.AdminMetadataManager"   :warn
   "org.apache.kafka.clients.consumer.ConsumerConfig"                :warn
   "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator" :warn
   "org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerInvoker" :warn
   "org.apache.kafka.clients.consumer.internals.Fetcher"             :error
   "org.apache.kafka.clients.consumer.internals.SubscriptionState"   :warn
   "org.apache.kafka.clients.consumer.KafkaConsumer"                 :warn
   "org.apache.kafka.clients.producer.KafkaProducer"                 :warn
   "org.apache.kafka.clients.producer.ProducerConfig"                :warn
   "org.apache.kafka.clients.producer.internals.Sender"              :off
   "org.apache.kafka.clients.producer.internals.TransactionManager"  :warn
   "org.apache.kafka.common.metrics.Metrics"                         :warn
   "org.apache.kafka.common.telemetry.internals.KafkaMetricsCollector" :warn
   "org.apache.kafka.common.utils.AppInfoParser"                     :warn})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a set of keyword faults."
  [spec]
  (let [special {"none"     #{}
                 "standard" #{:pause :kill :partition}
                 "all"      #{:pause :kill :partition}}]
    (->> (str/split spec #",")
         (remove #{""})
         (mapcat (fn [s] (or (get special s) [(keyword s)])))
         set)))

(defn stats-checker
  "A stats checker which ignores :crash and :debug-topic-partitions ops."
  []
  (let [c (checker/stats)]
    (reify checker/Checker
      (check [this test history opts]
        (let [res (checker/check c test history opts)]
          (if (every? :valid? (vals (dissoc (:by-f res)
                                            :debug-topic-partitions
                                            :crash)))
            (assoc res :valid? true)
            res))))))

(defn perf-checker
  "A perf checker which filters out debug/assign/crash ops."
  [perf-opts]
  (let [c (checker/perf perf-opts)]
    (reify checker/Checker
      (check [this test history opts]
        (checker/check c test
                       (h/remove (h/has-f? #{:assign
                                             :crash
                                             :debug-topic-partitions})
                                 history)
                       opts)))))

(defn queue-test
  "Constructs a Jepsen test for Camu's queue workload."
  [opts]
  (let [workload  (queue/workload opts)
        faults    (or (:nemesis opts) #{})
        nemesis   (if (empty? faults)
                    {:nemesis  (reify nemesis/Nemesis
                                 (setup! [this test] this)
                                 (invoke! [this test op] op)
                                 (teardown! [this test]))
                     :generator    nil
                     :final-generator nil}
                    {:nemesis         (nem/composed-nemesis faults)
                     :generator       (->> (gen/mix (nem/fault-cycles faults))
                                           (gen/stagger 5))
                     :final-generator (apply gen/phases
                                            (for [fault faults]
                                              (gen/nemesis
                                                (gen/once {:type :info
                                                           :f fault
                                                           :value :stop}))))})
        fg        (:final-generator workload)
        generator (-> (gen/phases
                        (->> (:generator workload)
                             (gen/stagger    (/ (:rate opts)))
                             (gen/nemesis    (:generator nemesis))
                             (gen/time-limit (:time-limit opts)))
                        (when (:final-generator nemesis)
                          (gen/nemesis (:final-generator nemesis)))
                        (when fg
                          (gen/phases
                            (gen/log "Waiting for recovery")
                            (gen/sleep 10)
                            (gen/time-limit (:final-time-limit opts)
                                            (gen/clients
                                              (:final-generator workload)))))))]
    (merge tests/noop-test
           opts
           {:name      (str "camu-queue"
                            (when (:txn? opts) " txn")
                            " " (->> opts :sub-via (map name) sort
                                     (str/join ","))
                            (when-let [a (:acks opts)] (str " acks=" a))
                            (when (contains? opts :idempotence)
                              (str " idem=" (:idempotence opts)))
                            (when (seq faults)
                              (str " " (->> faults (map name) sort
                                            (str/join ",")))))
            :os        os/noop
            :db        (db/db)
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator generator
            :checker   (checker/compose
                         {:stats    (stats-checker)
                          :perf     (perf-checker {})
                          :ex       (checker/unhandled-exceptions)
                          :workload (:checker workload)})
            :logging   {:overrides logging-overrides}})))

(def cli-opts
  "CLI options for the queue test."
  [[nil "--abort-p PROBABILITY" "Probability of aborting a transaction."
    :default 0.1
    :parse-fn read-string
    :validate [#(<= 0 % 1) "must be between 0 and 1"]]

   [nil "--acks ACKS" "Producer acknowledgement level. Default uses client default; try 1 or 'all'."
    :default nil]

   [nil "--auto-commit-interval MILLIS" "Auto-commit interval in ms."
    :default 5
    :parse-fn parse-long]

   [nil "--auto-offset-reset BEHAVIOR" "Consumer offset reset behavior."
    :default nil]

   [nil "--crash-clients" "Periodically crash clients."
    :default false]

   [nil "--crash-client-interval SECONDS" "Client crash interval."
    :default 30
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be positive"]]

   [nil "--camu-binary PATH" "Path to camu binary."
    :default "/jepsen/camu/camu"]

   [nil "--s3-endpoint URL" "MinIO/S3 endpoint."
    :default "http://minio:9000"]

   [nil "--http-port PORT" "HTTP port for camu."
    :default 8080
    :parse-fn #(Integer/parseInt %)]

   [nil "--kafka-port PORT" "Kafka port for camu."
    :default 9092
    :parse-fn #(Integer/parseInt %)]

   [nil "--[no-]idempotence" "Enable producer idempotence."]

   [nil "--intra-txn-delay MS" "Delay between micro-ops in a transaction."
    :default  0
    :parse-fn parse-long]

   [nil "--isolation-level NAME" "Consumer isolation level, e.g. read_committed."]

   [nil "--final-time-limit SECONDS" "Max time for final generator phase."
    :default  200
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be positive"]]

   [nil "--max-writes-per-key LIMIT" "Max writes per key."
    :default 1024
    :parse-fn parse-long
    :validate [pos? "must be positive"]]

   [nil "--nemesis FAULTS" "Comma-separated nemesis faults: none, kill, partition, pause, etc."
    :parse-fn parse-nemesis-spec]

   [nil "--rate HZ" "Target ops/sec."
    :default  100
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "must be non-negative"]]

   [nil "--retries COUNT" "Producer retries."
    :parse-fn util/parse-long]

   [nil "--sub-via STRATEGIES" "Comma-separated: assign and/or subscribe."
    :default #{:assign}
    :parse-fn (comp set (fn [s] (map keyword (str/split s #","))))
    :validate [#(every? #{:assign :subscribe} %)
               "must be assign and/or subscribe"]]

   [nil "--[no-]txn" "Enable transactions."
    :id :txn?]

   [nil "--[no-]ww-deps" "Enable write-write dependency inference."
    :default true]

   [nil "--enable-auto-commit" "Enable automatic consumer commits."
    :default  nil
    :assoc-fn (fn [m _ _] (assoc m :enable-auto-commit true))]

   [nil "--disable-auto-commit" "Disable automatic consumer commits."
    :assoc-fn (fn [m _ _] (assoc m :enable-auto-commit false))]

   [nil "--faults FAULTS" "Comma-separated fault types for Camu nemesis."
    :default #{:kill}
    :parse-fn (fn [s] (set (map keyword (str/split s #","))))]])

(defn -main
  "CLI entry point for queue tests."
  [& args]
  (cli/run!
    (merge (cli/single-test-cmd {:test-fn  queue-test
                                 :opt-spec cli-opts})
           (cli/serve-cmd))
    args))
