(ns jepsen.camu.kafka-client
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.camu.client :as http-client]
            [jepsen [client :as client]])
  (:import (java.time Duration)
           (java.util Properties UUID)
           (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic)
           (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord RecordMetadata)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.errors TopicExistsException)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)))

(def kafka-timeout-ms 5000)

(defn kafka-port
  [test]
  (get test :kafka-port 9092))

(defn bootstrap-servers
  [test]
  (str/join "," (map #(str % ":" (kafka-port test)) (:nodes test))))

(defn ^Properties ->properties
  [m]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (.put props k (str v)))
    props))

(defn producer-config
  [test]
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (bootstrap-servers test)
   ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)
   ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (.getName StringSerializer)
   ProducerConfig/ACKS_CONFIG "all"
   ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG "true"
   ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG "10000"
   ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG "5000"
   ProducerConfig/MAX_BLOCK_MS_CONFIG "10000"
   ProducerConfig/RETRIES_CONFIG "3"})

(defn consumer-config
  [test group-id]
  {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG (bootstrap-servers test)
   ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/GROUP_ID_CONFIG group-id
   ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG "false"
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"
   ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG "10000"
   ConsumerConfig/DEFAULT_API_TIMEOUT_MS_CONFIG "10000"
   ConsumerConfig/MAX_POLL_RECORDS_CONFIG "1000"
   ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG "6000"})

(defn admin-config
  [test]
  {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG (bootstrap-servers test)
   AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG "10000"
   AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG "30000"})

(defn open-admin
  [test]
  (AdminClient/create (->properties (admin-config test))))

(defn- create-topic-request
  "Builds a NewTopic from opts."
  [topic opts]
  (let [partitions (get opts :num-partitions 4)
        rf         (short (get opts :replication-factor 1))
        new-topic  (NewTopic. topic (int partitions) rf)
        configs    (cond-> {}
                     (> rf 1) (assoc "min.insync.replicas"
                                     (str (get opts :min-insync-replicas 1))))]
    (when (seq configs)
      (.configs new-topic configs))
    new-topic))

(defn ensure-topic-created!
  "Retries topic creation via Kafka AdminClient until it succeeds."
  [test topic opts timeout-ms]
  (let [deadline  (+ (System/currentTimeMillis) timeout-ms)
        new-topic (create-topic-request topic opts)]
    (with-open [admin (open-admin test)]
      (loop []
        (let [created? (try
                         (.. admin (createTopics [new-topic]) (all) (get))
                         (info "Topic" topic "created via Kafka AdminClient")
                         true
                         (catch ExecutionException e
                           (if (instance? TopicExistsException (.getCause e))
                             (do (info "Topic" topic "already exists")
                                 true)
                             (do (warn "Kafka AdminClient create-topic failed:"
                                       (.getMessage (.getCause e)))
                                 false)))
                         (catch Exception e
                           (warn "Kafka AdminClient create-topic failed:" (.getMessage e))
                           false))]
          (cond
            created?
            (info "Topic" topic "ready (Kafka protocol)")

            (> (System/currentTimeMillis) deadline)
            (throw (ex-info "timed out creating topic via Kafka AdminClient"
                            {:type :topic-not-ready :topic topic}))

            :else
            (do (Thread/sleep 1000)
                (recur))))))))

(defn open-producer
  [test]
  (KafkaProducer. (->properties (producer-config test))))

(defn open-consumer
  [test group-id]
  (KafkaConsumer. (->properties (consumer-config test group-id))))

(defn- execution-root-cause
  [^ExecutionException e]
  (or (.getCause e) e))

(defn wait-for-kafka-topic-metadata-ready!
  [test topic timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (with-open [producer (open-producer test)]
      (loop []
        (let [ready?
              (try
                (let [partitions (.partitionsFor producer topic)]
                  (boolean
                   (when (and (= (count partitions) (get test :num-partitions 4))
                              (every? #(some? (.leader %)) partitions))
                     (info "Kafka topic" topic "metadata ready")
                     true)))
                (catch ExecutionException e
                  (warn "Kafka metadata readiness check failed for" topic
                        (.getMessage (execution-root-cause e)))
                  false)
                (catch Exception e
                  (warn "Kafka metadata readiness check failed for" topic
                        (.getMessage e))
                  false))]
          (cond
            ready?
            true

            (> (System/currentTimeMillis) deadline)
            (throw (ex-info "kafka topic metadata readiness timeout"
                            {:type :topic-not-ready
                             :topic topic}))

            :else
            (do
              (Thread/sleep 1000)
              (recur))))))))

(defn send-produce!
  [^KafkaProducer producer topic partition key value]
  (let [record (ProducerRecord. topic (int partition) key value)
        ^RecordMetadata meta (.get (.send producer record))]
    {:partition (.partition meta)
     :offset (.offset meta)}))

(defn- records->messages
  [records]
  (mapv (fn [record]
          {:offset (.offset record)
           :partition (.partition record)
           :key (.key record)
           :value (.value record)})
        records))

(defn consume-once!
  [^KafkaConsumer consumer topic partition offset timeout-ms]
  (let [tp (TopicPartition. topic (int partition))]
    (.assign consumer [tp])
    (.seek consumer tp (long offset))
    {:messages (records->messages (.records (.poll consumer (Duration/ofMillis timeout-ms)) tp))
     :node nil}))

(defn drain-partition!
  [^KafkaConsumer consumer topic partition offset]
  (let [tp (TopicPartition. topic (int partition))
        deadline (+ (System/currentTimeMillis) http-client/drain-timeout-ms)]
    (.assign consumer [tp])
    (.seek consumer tp (long offset))
    (loop [acc []
           empty-polls 0]
      (let [records (.records (.poll consumer (Duration/ofMillis 500)) tp)
            batch (records->messages records)]
        (cond
          (seq batch)
          (recur (into acc batch) 0)

          (or (>= empty-polls 1)
              (> (System/currentTimeMillis) deadline))
          acc

          :else
          (recur acc (inc empty-polls)))))))

(defn- do-produce
  "Shared produce logic for :produce and :idempotent-produce ops."
  [producer topic num-partitions op]
  (let [{:keys [key value partition]} (:value op)
        partition (or partition (http-client/route-partition key num-partitions))]
    (assoc op :type :ok :value (merge {:key key :value value :partition partition}
                                      (send-produce! producer topic partition key value)))))

(defrecord KafkaClient [node producer consumer topic]
  client/Client
  (open! [this test node']
    (let [group-id (str "jepsen-kafka-" (UUID/randomUUID))]
      (assoc this
             :node node'
             :group-id group-id
             :num-partitions (get test :num-partitions 4)
             :producer (open-producer test)
             :consumer (open-consumer test group-id))))

  (setup! [this test]
    (let [topic (http-client/topic-name this test)]
      (when (= node (first (:nodes test)))
        (ensure-topic-created!
         test
         topic
         (select-keys test [:replication-factor :min-insync-replicas :num-partitions])
         60000))
      (wait-for-kafka-topic-metadata-ready! test topic 60000))
    this)

  (invoke! [this test op]
    (let [topic (http-client/topic-name this test)]
      (try
        (case (:f op)
          (:produce :idempotent-produce)
          (do-produce producer topic (:num-partitions this) op)

          :consume
          (let [{:keys [partition offset]} (:value op)
                result (consume-once! consumer topic partition (or offset 0) 1000)
                messages (:messages result)]
            (when-let [offsets-atom (:consume-offsets test)]
              (when (seq messages)
                (let [max-offset (apply max (map :offset messages))]
                  (swap! offsets-atom update partition #(max (or % 0) (inc max-offset))))))
            (assoc op :type :ok :value {:partition partition :messages messages}))

          :drain
          (let [{:keys [partition offset]} (:value op)
                messages (drain-partition! consumer topic partition (or offset 0))]
            (assoc op :type :ok :value {:partition partition :messages messages}))

          (assoc op :type :fail :error :unsupported-operation))

        (catch ExecutionException e
          (assoc op :type :info :error (.getMessage (execution-root-cause e))))
        (catch Exception e
          (assoc op :type :info :error (.getMessage e))))))

  (teardown! [this test]
    this)

  (close! [this test]
    (when producer
      (.close ^KafkaProducer producer (Duration/ofMillis kafka-timeout-ms)))
    (when consumer
      (.wakeup ^KafkaConsumer consumer)
      (try
        (.close ^KafkaConsumer consumer (Duration/ofMillis kafka-timeout-ms))
        (catch Exception _)))))

(defn client
  []
  (->KafkaClient nil nil nil nil))
