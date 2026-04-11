(ns jepsen.camu.kafka
  "Wrapper for the Java Kafka client. Ported from jepsen.redpanda.client."
  (:require [clojure.core.protocols :refer [Datafiable datafy]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt]
            [jepsen.util :as util :refer [await-fn
                                          map-vals
                                          pprint-str]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.time Duration)
           (java.util Properties)
           (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin Admin
                                           AdminClientConfig
                                           NewTopic)
           (org.apache.kafka.clients.consumer ConsumerConfig
                                              ConsumerRebalanceListener
                                              ConsumerRecord
                                              ConsumerRecords
                                              KafkaConsumer
                                              OffsetAndMetadata)
           (org.apache.kafka.clients.producer KafkaProducer
                                              ProducerConfig
                                              ProducerRecord)
           (org.apache.kafka.common KafkaException
                                    TopicPartition)
           (org.apache.kafka.common.errors InvalidTopicException
                                           TopicExistsException
                                           WakeupException)))

(def port
  "What port do we connect to?"
  9092)

(def next-transactional-id
  "We automatically assign each producer a unique transactional ID"
  (atom -1))

(defn new-transactional-id
  "Returns a unique transactional ID (mutating the global counter)"
  []
  (str "jt" (swap! next-transactional-id inc)))

(defn ^Properties ->properties
  "Turns a map into a Properties object."
  [m]
  (doto (Properties.)
    (.putAll (map-vals str m))))

(def consumer-config-logged?
  "Used to ensure that we only log consumer configs once."
  (atom false))

(def producer-config-logged?
  "Used to ensure that we only log producer configs once."
  (atom false))

(def consumer-group
  "Right now all consumers are a single consumer group."
  "jepsen-group")

(extend-protocol Datafiable
  TopicPartition
  (datafy [x]
    {:topic     (.topic x)
     :partition (.partition x)}))

(defn consumer-config
  "Constructs a properties map for talking to a given Kafka node."
  [node opts]
  (cond->
    {ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG
     "org.apache.kafka.common.serialization.LongDeserializer"

     ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG
     "org.apache.kafka.common.serialization.LongDeserializer"

     ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG
     (str node ":" port)

     ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG
     1000

     ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG
     500

     ConsumerConfig/METADATA_MAX_AGE_CONFIG
     60000

     ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG
     10000

     ConsumerConfig/DEFAULT_API_TIMEOUT_MS_CONFIG
     10000

     ConsumerConfig/HEARTBEAT_INTERVAL_MS_CONFIG
     300

     ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG
     6000

     ConsumerConfig/CONNECTIONS_MAX_IDLE_MS_CONFIG
     60000
     }
    true
    (assoc ConsumerConfig/GROUP_ID_CONFIG consumer-group)

    (not= nil (:isolation-level opts))
    (assoc ConsumerConfig/ISOLATION_LEVEL_CONFIG (:isolation-level opts))

    (not= nil (:auto-offset-reset opts))
    (assoc ConsumerConfig/AUTO_OFFSET_RESET_CONFIG (:auto-offset-reset opts))

    (not= nil (:enable-auto-commit opts))
    (assoc ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG (:enable-auto-commit opts))

    (not= nil (:max-poll-records opts))
    (assoc ConsumerConfig/MAX_POLL_RECORDS_CONFIG (:max-poll-records opts))

    (not= nil (:auto-commit-interval opts))
    (assoc ConsumerConfig/AUTO_COMMIT_INTERVAL_MS_CONFIG (:auto-commit-interval opts))))

(defn producer-config
  "Constructs a config map for talking to a given node."
  [node opts]
  (cond-> {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG
           (str node ":" port)

           ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
           "org.apache.kafka.common.serialization.LongSerializer"

           ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
           "org.apache.kafka.common.serialization.LongSerializer"

           ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG 10000
           ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG 3000
           ProducerConfig/MAX_BLOCK_MS_CONFIG 10000
           ProducerConfig/TRANSACTION_TIMEOUT_CONFIG
           1000
           ProducerConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG 1000
           ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG 500
           ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000

           }
    (not= nil (:acks opts))
    (assoc ProducerConfig/ACKS_CONFIG (:acks opts))

    (not= nil (:idempotence opts))
    (assoc ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG (:idempotence opts))

    (not= nil (:retries opts))
    (assoc ProducerConfig/RETRIES_CONFIG (:retries opts))

    (not= nil (:client-id opts))
    (assoc ProducerConfig/CLIENT_ID_CONFIG (:client-id opts))

    (not= nil (:transactional-id opts))
    (assoc ProducerConfig/TRANSACTIONAL_ID_CONFIG (:transactional-id opts))))

(defn admin-config
  "Constructs a config map for an admin client connected to the given node."
  [node]
  {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG       (str node ":" port)
   AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG                 3000
   AdminClientConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG               1000
   AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG                     3000
   AdminClientConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG     500
   AdminClientConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000
   AdminClientConfig/RETRIES_CONFIG                                0})

(defn ^Duration ms->duration
  "Constructs a Duration from millis."
  [ms]
  (Duration/ofMillis ms))

(defn close!
  "Closes any AutoCloseable."
  [^java.lang.AutoCloseable c]
  (.close c))

(defn close-consumer!
  "Closes a consumer *immediately*."
  [^KafkaConsumer c]
  (let [killer (future
                 (util/with-thread-name "jepsen kafka close killer"
                   (Thread/sleep 1000)
                   (.wakeup c)))]
    (let [r (try
              (.close c (ms->duration 0))
              :closed
              (catch WakeupException _
                :wakeup))]
      (future-cancel killer)
      r)))

(defn close-producer!
  "Closes a producer *immediately*, without waiting for incomplete requests."
  [^KafkaProducer p]
  (.close p (ms->duration 0)))

(defn consumer
  "Opens a new consumer for the given node."
  [opts node]
  (let [config (consumer-config node opts)]
    (when (compare-and-set! consumer-config-logged? false true)
      (info "Consumer config:\n" (pprint-str config)))
    (KafkaConsumer. (->properties config))))

(defn producer*
  "Opens a new producer for a node. Doesn't initialize transactions."
  [opts node]
  (let [config (producer-config node opts)]
    (when (compare-and-set! producer-config-logged? false true)
      (info "Producer config:\n" (pprint-str config)))
    (KafkaProducer. (->properties config))))

(defn producer
  "Opens a new producer for a node. Automatically initializes transactions, if
  :transactional-id opts is set."
  [opts node]
  (if-not (:transactional-id opts)
    (producer* opts node)
    (await-fn (fn init-txns []
                (let [p (producer* opts node)]
                  (try (.initTransactions p)
                       p
                       (catch Throwable t
                         (close-producer! p)
                         (throw t)))))
              {:log-interval 5000
               :log-message "Waiting for initTransactions()"})))

(defn admin
  "Opens an admin client for a node."
  [test node]
  (Admin/create (->properties (admin-config node))))

(defn create-topic!
  "Creates a new topic using an admin client. Synchronous. If the topic already
  exists, returns :already-exists instead of throwing."
  [^Admin admin name partitions replication-factor]
  (try
    (let [topic (NewTopic. ^String name,
                           ^int partitions,
                           ^short replication-factor)
          res   (.createTopics admin [topic])]
      (.. res values (get name) get))
    (catch java.util.concurrent.ExecutionException e
      (condp instance? (util/ex-root-cause e)
        TopicExistsException :already-exists
        (throw e)))))

(defn ^TopicPartition topic-partition
  "A tuple of a topic and a partition number together."
  [topic partition]
  (TopicPartition. topic partition))

(defn ^ProducerRecord producer-record
  "Constructs a ProducerRecord from a topic, partition, key, and value."
  [topic partition key value]
  (ProducerRecord. topic (int partition) key value))

(defn ^OffsetAndMetadata offset+metadata
  "Constructs an OffsetAndMetadata."
  [^long offset]
  (OffsetAndMetadata. offset))

(defn subscribe!
  "Subscribes to the given set of topics."
  ([^KafkaConsumer consumer, topics]
   (.subscribe consumer topics))
  ([^KafkaConsumer consumer, topics, rebalance-listener]
   (.subscribe consumer topics rebalance-listener)))

(defn poll-up-to
  "Takes a consumer, and polls it (with duration 0) for records up to and
  including (dec offset), and (quite possibly) higher. Returns a lazy sequence
  of ConsumerRecords."
  ([consumer offset]
   (poll-up-to consumer offset (ms->duration 10)))
  ([^KafkaConsumer consumer offset duration]
   (when (pos? offset)
     (let [records     (.poll consumer duration)
           records     (vec records)
           last-record ^ConsumerRecord (peek records)]
       (cond (nil? last-record)
             (poll-up-to consumer offset (ms->duration 100))

             (<= (dec offset) (.offset last-record))
             records

             true
             (concat records
                     (lazy-seq (poll-up-to consumer offset duration))))))))

(defn ^KafkaConsumer reset-to-last-committed-positions!
  "Takes a Consumer, and seeks back to the last offsets that were committed.
  Returns consumer."
  [^KafkaConsumer consumer]
  (let [assignment (.assignment consumer)
        committed  (.committed consumer assignment)]
    (doseq [^TopicPartition topic-partition assignment]
      (if-let [^OffsetAndMetadata offset+metadata
               (.get committed topic-partition)]
        (.seek consumer topic-partition (.offset offset+metadata))
        (.seekToBeginning [topic-partition]))))
   consumer)

(defn abort-txn!
  "Aborts a transaction."
  [^KafkaProducer producer]
  (.abortTransaction producer))

(defmacro unwrap-errors
  "Depending on whether you're doing a future get or a sync call, Kafka might
  throw its exceptions wrapped in a j.u.c.ExecutionException. This macro
  transparently unwraps those."
  [& body]
  `(try ~@body
        (catch ExecutionException e#
          (let [cause# (util/ex-root-cause e#)]
            (if (instance? KafkaException cause#)
              (throw cause#)
              (throw e#))))))

(defn panicky-rebalance-listener
  "A ConsumerRebalanceListener which throws at the drop of a hat."
  []
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [_ topic-partitions]
      (throw+ {:type       :partitions-revoked
               :partitions (mapv datafy topic-partitions)}))

    (onPartitionsAssigned [_ topic-partitions]
      (throw+ {:type       :partitions-assigned
               :partitions (mapv datafy topic-partitions)}))

    (onPartitionsLost [_ topic-partitions]
      (throw+ {:type       :partitions-lost
               :partitions (mapv datafy topic-partitions)}))))

(defn logging-rebalance-listener
  "A rebalance listener which journals each event to an atom containing a
  vector."
  [log-atom]
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [_ topic-partitions]
      (swap! log-atom conj {:type :revoked
                            :partitions (mapv datafy topic-partitions)}))

    (onPartitionsAssigned [_ topic-partitions]
      (swap! log-atom conj {:type       :assigned
                            :partitions (mapv datafy topic-partitions)}))

    (onPartitionsLost [_ topic-partitions]
      (swap! log-atom conj {:type       :lost
                            :partitions (mapv datafy topic-partitions)}))))
