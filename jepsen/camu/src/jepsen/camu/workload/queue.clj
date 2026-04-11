(ns jepsen.camu.workload.queue
  "A workload which treats Camu as a queue. Each client maintains a
  producer and consumer. Ported from jepsen.redpanda.workload.queue.

  To subscribe to a new set of topics, we issue an operation like:

    {:f :subscribe, :value [k1, k2, ...]}

  or

    {:f :assign, :value [k1, k2, ...]}

  ... where k1, k2, etc denote specific topics and partitions.

  Reads and writes (and mixes thereof) are encoded as a vector of
  micro-operations:

    {:f :poll, :value [op1, op2, ...]}
    {:f :send, :value [op1, op2, ...]}
    {:f :txn,  :value [op1, op2, ...]}

  From this history we perform Elle-based queue consistency analysis."
  (:require [analemma [xml :as xml]
                      [svg :as svg]]
            [bifurcan-clj [core :as b]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ real-pmap loopr]]
            [elle [core :as elle]
                  [graph :as g]
                  [list-append :refer [rand-bg-color]]
                  [txn :as txn]
                  [util :refer [index-of]]
                  [rels :refer [ww wr rw]]]
            [gnuplot.core :as gnuplot]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [util :as util :refer [map-vals
                                           meh
                                           nanos->secs
                                           pprint-str]]]
            [jepsen.checker.perf :as perf]
            [jepsen.tests kafka]
            [jepsen.camu [kafka :as rc]]
            [knossos [history :as history]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin Admin)
           (org.apache.kafka.clients.consumer ConsumerRecords
                                              ConsumerRecord
                                              KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer
                                              RecordMetadata)
           (org.apache.kafka.common KafkaException
                                    TopicPartition)
           (org.apache.kafka.common.errors AuthorizationException
                                           DisconnectException
                                           InterruptException
                                           InvalidProducerEpochException
                                           InvalidReplicationFactorException
                                           InvalidTopicException
                                           InvalidTxnStateException
                                           NetworkException
                                           NotControllerException
                                           NotLeaderOrFollowerException
                                           OutOfOrderSequenceException
                                           ProducerFencedException
                                           TimeoutException
                                           UnknownTopicOrPartitionException
                                           UnknownServerException
                                           )))

(def default-abort-p
  "What's the probability that we abort a transaction at any given step?"
  1/32)

(def partition-count
  "How many partitions per topic?"
  2)

(def replication-factor
  "What replication factor should we use for each topic?"
  3)

(def poll-ms
  "How long should we poll for, in ms?"
  100)

(defn k->topic
  "Turns a logical key into a topic."
  [k]
  (str "t" (quot k partition-count)))

(defn k->partition
  "Turns a logical key into a partition within a topic."
  [k]
  (mod k partition-count))

(defn k->topic-partition
  "Turns a logical key into a TopicPartition."
  [k]
  (rc/topic-partition (k->topic k) (k->partition k)))

(defn topic-partition->k
  "Turns a TopicPartition into a key."
  ([^TopicPartition tp]
   (topic-partition->k (.topic tp) (.partition tp)))
  ([topic partition]
   (+ (* partition-count (parse-long (nth (re-find #"t(\d+)" topic) 1)))
      partition)))

(defn mop!
  "Applies a micro-operation from a transaction: either a :r read or a :append
  operation."
  [{:keys [extant-topics
           ^Admin admin
           ^KafkaProducer producer
           ^KafkaConsumer consumer] :as client}
   poll-ms
   mop]
  (case (first mop)
    :poll (try
            (rc/unwrap-errors
              (let [records (.poll consumer (rc/ms->duration poll-ms))]
                (->> (.partitions records)
                     (map (fn per-topic-partition [topic-partition]
                            [(topic-partition->k topic-partition)
                             (mapv (fn xform-messages [^ConsumerRecord record]
                                     [(.offset record)
                                      (.value record)])
                                   (.records records topic-partition))]))
                     (into (sorted-map))
                     (vector :poll))))
              (catch InvalidTopicException _
                [:poll {}])
              (catch IllegalStateException e
                (if (re-find #"not subscribed to any" (.getMessage e))
                  [:poll {}]
                  (throw e))))

    :send (let [[f k v]   mop
                topic     (k->topic k)
                _         (when-not (contains? @extant-topics topic)
                            (rc/create-topic! admin topic partition-count
                                              replication-factor)
                            (swap! extant-topics conj topic))
                partition (k->partition k)
                record (rc/producer-record topic (k->partition k) nil v)
                res    ^RecordMetadata (-> producer
                                           (.send record)
                                           (deref 10000 nil)
                                           (or (throw+ {:type :timeout})))
                k'     (topic-partition->k (.topic res)
                                           (.partition res))
                offset (when (.hasOffset res)
                         (.offset res))]
            (assert+ (= k k')
                     {:type ::key-mismatch
                      :k  k
                      :k' k'})
            (when-not offset
              (info "Missing offset for send() acknowledgement of key" k "value" v))
            [f k' [offset v]])))

(defn rollback-consumer!
  "Takes a consumer and a completed txn op with a mutable value. Rolls back the
  consumer positions to the first ones read in this op. Returns op."
  [^KafkaConsumer consumer op]
  (loopr [offsets {}]
         [[_ poll]  (->> op :value deref (filter (comp #{:poll} first)))
          [k pairs] poll]
         (recur (if (contains? offsets k)
                  offsets
                  (assoc offsets k (first (first pairs)))))
         (doseq [[k offset] offsets]
           (try
             (.seek consumer (k->topic-partition k) offset)
             (catch IllegalStateException e
               nil))))
  op)

(defmacro with-consumer-rollback
  "Takes a client, an op with a mutable :value atom, and a body. Evaluates body
  to produce a completion op. If the op has :type :info or :fail, or if it
  throws, rolls back the client's consumer position."
  [client op & body]
  (assert (symbol? op))
  `(try
     (let [op'# (do ~@body)]
       (if (= :ok (:type op'#))
         op'#
         (rollback-consumer! (:consumer ~client) op'#)))
     (catch Throwable t#
       (rollback-consumer! (:consumer ~client) ~op)
       (throw t#))))

(defn send-offsets!
  "Takes a client and a completed txn operation. Finds the highest polled
  offsets from that op's :value atom, and calls .sendOffsetsToTransaction on
  the producer."
  [client op]
  (loopr [offsets {}]
         [[_ poll]       (->> op :value deref (filter (comp #{:poll} first)))
          [k pairs]      poll
          [offset value] pairs]
         (recur (update offsets k (fnil max ##-Inf) offset))
         (let [kafka-offsets (->> (for [[k offset] offsets]
                                    [(k->topic-partition k)
                                     (rc/offset+metadata (inc offset))])
                                  (into {}))
               producer ^KafkaProducer (:producer client)
               consumer ^KafkaConsumer (:consumer client)]
           (when-not (empty? offsets)
             (.sendOffsetsToTransaction
               producer
               kafka-offsets
               (.groupMetadata consumer))))))

(defn safe-abort!
  "Tries to abort the transaction on the current producer. Throws a map
  describing the outcome."
  [client tried-commit?, body-error]
  (try (rc/abort-txn! (:producer client))
       (catch RuntimeException abort-error
         (throw+ {:type           :abort
                  :abort-ok?      false
                  :tried-commit?  tried-commit?
                  :definite?
                  (or (not tried-commit?)
                      (instance? InvalidTxnStateException body-error))
                  :body-error     body-error
                  :abort-error    abort-error})))
  (throw+ {:type           :abort
           :abort-ok?      true
           :tried-commit?  tried-commit?
           :definite?      true
           :body-error     body-error}))

(defmacro with-txn
  "Takes a test, a client, an operation, and a body. If (:txn? test) is false,
  evaluates body. If true, evaluates body in a transaction."
  [test client op & body]
  (assert (symbol? op))
  (let [definite-non-abortable-catches
        `[(catch ProducerFencedException     e# (throw e#))
          (catch OutOfOrderSequenceException e# (throw e#))
          (catch AuthorizationException      e# (throw e#))]]
    `(if-not (:txn? ~test)
       (do ~@body)
       (let [producer# ^KafkaProducer (:producer ~client)]
         (.beginTransaction producer#)
         (let [op'# (try (let [op'# (do ~@body)]
                           (send-offsets! ~client op'#)
                           op'#)
                         ~@definite-non-abortable-catches
                         (catch RuntimeException body-err#
                           (safe-abort! ~client false body-err#)))]
           (try (.commitTransaction producer#)
                ~@definite-non-abortable-catches
                (catch TimeoutException e#
                  (throw e#))
                (catch InterruptException e#
                  (throw e#))
                (catch RuntimeException e#
                  (safe-abort! ~client true e#)))
           op'#)))))

(defn serializable-exception
  "Makes an exception safe for representation in our data structures."
  [e]
  (if (instance? clojure.lang.ExceptionInfo e)
    (ex-data e)
    (str e)))

(defmacro with-errors
  "Takes an operation and a body. Evaluates body, catching common exceptions
  and returning appropriate fail/info operations when they occur."
  [op & body]
  (assert (symbol? op))
  `(try+ ~@body
          (catch AuthorizationException _#
            (assoc ~op
                   :type         :fail
                   :error        :authorization
                   :end-process? true))

          (catch DisconnectException e#
           (assoc ~op :type :info, :error [:disconnect (.getMessage e#)]))

          (catch IllegalStateException e#
            (condp re-find (.getMessage e#)
              #"Invalid transition attempted"
              (assoc ~op
                     :type :info
                     :error [:illegal-transition (.getMessage e#)])))

         (catch InvalidProducerEpochException e#
           (assoc ~op
                  :type  :fail
                  :error [:invalid-producer-epoch (.getMessage e#)]))

         (catch InvalidTopicException _#
           (assoc ~op :type :fail, :error :invalid-topic))

         (catch InvalidReplicationFactorException _#
           (assoc ~op :type :fail :error :invalid-replication-factor))

         (catch NetworkException e#
           (assoc ~op :type :info, :error [:network (.getMessage e#)]))

         (catch NotControllerException e#
           (assoc ~op :type :fail, :error :not-controller))

         (catch NotLeaderOrFollowerException _#
           (assoc ~op :type :info, :error :not-leader-or-follower))

         (catch OutOfOrderSequenceException _#
            (assoc ~op
                   :type         :fail
                   :error        :out-of-order-sequence
                   :end-process? true))

         (catch ProducerFencedException _#
            (assoc ~op
                   :type         :fail,
                   :error        :producer-fenced
                   :end-process? true))

         (catch UnknownTopicOrPartitionException _#
           (assoc ~op :type :fail, :error :unknown-topic-or-partition))

         (catch UnknownServerException e#
           (assoc ~op :type :info, :error [:unknown-server-exception
                                          (.getMessage e#)]))

         (catch TimeoutException _#
           (assoc ~op :type :info, :error :kafka-timeout))

         (catch KafkaException e#
           (condp re-find (.getMessage e#)
             #"broker is not available"
             (assoc ~op :type :fail, :error :broker-not-available)

             #"Cannot execute transactional method because we are in an error state"
             (assoc ~op
                    :type         :fail
                    :error        [:txn-in-error-state (.getMessage e#)]
                    :end-process? true)

             #"Topic or Partition .+? does not exist"
             (assoc ~op :type :fail, :error [:topic-partition-does-not-exist (.getMessage e#)])

             #"Unexpected error in AddOffsetsToTxnResponse"
             (assoc ~op :type :fail, :error [:add-offsets (.getMessage e#)])

             #"Unexpected error in TxnOffsetCommitResponse"
             (assoc ~op
                    :type  :fail
                    :error [:txn-offset-commit (.getMessage e#)])

             #"Unhandled error in EndTxnResponse"
             (assoc ~op :type :info, :error [:end-txn (.getMessage e#)])

             (throw e#)))

         (catch [:type :abort] e#
           (assoc ~op
                  :type         (if (:definite? e#) :fail :info)
                  :end-process? (not (:abort-ok? e#))
                  :error        (cond-> e#
                                  true
                                  (update :body-error serializable-exception)

                                  (:abort-error e#)
                                  (update :abort-error serializable-exception))))

         (catch [:type :partitions-assigned] e#
           (assoc ~op :type :fail, :error e#))

         (catch [:type :partitions-lost] e#
           (assoc ~op :type :fail, :error e#))

         (catch [:type :partitions-revoked] e#
           (assoc ~op :type :fail, :error e#))

         (catch [:type :timeout] e#
           (assoc ~op :type :info, :error :timeout))))

(defmacro with-rebalance-log
  "Tracks rebalance events during a transaction body."
  [client & body]
  `(let [log# (:rebalance-log ~client)]
     (reset! log# [])
     (let [op'# ~@body
           log# (map (fn [entry#]
                       (-> entry#
                           (dissoc :partitions)
                           (assoc :keys (mapv (fn [tp#]
                                                (topic-partition->k
                                                  (:topic tp#)
                                                  (:partition tp#)))
                                             (:partitions entry#)))))
                     @log#)]
       (if (seq log#)
         (assoc op'# :rebalance-log log#)
         op'#))))

(defmacro with-mutable-value
  "Takes a symbol referring to an invocation operation, and evaluates body
  where `op` has a :value which is an atom wrapping the original op's :value."
  [op & body]
  (assert (symbol? op))
  `(let [value# (atom (:value ~op))
         ~op    (assoc ~op :value value#)
         op'#   (do ~@body)]
     (assoc op'# :value @value#)))

(defn maybe-abort
  "Intentionally throws inside transactions with probability (:abort-p test)."
  [test]
  (when (and (:txn? test)
             (< (rand) (:abort-p test default-abort-p)))
    (throw+ {:type :intentional-abort})))

(defrecord Client [node
                   ^Admin admin
                   ^KafkaProducer producer
                   ^KafkaConsumer consumer
                   extant-topics
                   rebalance-log]
  client/Client
  (open! [this test node]
    (let [tx-id    (rc/new-transactional-id)
          producer-opts (assoc test :client-id (str "jepsen-" tx-id))
          producer (rc/producer
                     (if (:txn? test)
                       (assoc test :transactional-id tx-id)
                       test)
                     node)]
      (info "transactional-id" tx-id)
      (assoc this
             :node          node
             :admin         (rc/admin test node)
             :consumer      (rc/consumer test node)
             :producer      producer
             :rebalance-log (atom []))))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      ; Assign this consumer new topic-partitions
      :assign (let [tps (map k->topic-partition (:value op))]
                (.assign consumer tps)
                (when (:seek-to-beginning? op)
                  (info "Seeking to beginning")
                  (.seekToBeginning consumer tps))
                (assoc op :type :ok))

      ; Crash this client, forcing us to open a new client
      :crash     (assoc op :type :info)

      ; Debug topic partitions - stubbed for Camu (no equivalent admin API)
      :debug-topic-partitions
      (assoc op :type :fail, :error :not-supported)

      ; Subscribe to the topics containing these keys
      :subscribe
      (let [topics (->> (:value op)
                        (map k->topic)
                        distinct)]
        (if (:txn? test)
          (rc/subscribe! consumer
                         topics
                         (rc/logging-rebalance-listener rebalance-log))
          (rc/subscribe! consumer topics))
        (assoc op :type :ok))

      ; Apply poll/send transactions.
      (:poll, :send, :txn)
      (let [sleep (if (< 0.5 (rand))
                    0
                    (rand-int (:intra-txn-delay test 0)))]
        (with-mutable-value op
          (with-consumer-rollback this op
            (with-rebalance-log this
              (with-errors op
                (with-txn test this op
                  (rc/unwrap-errors
                    (do (reduce (fn [i mop]
                                  (maybe-abort test)
                                  (let [mop' (mop! this
                                                   (:poll-ms op poll-ms)
                                                   mop)]
                                    (swap! (:value op) assoc i mop'))
                                  (when (pos? sleep) (Thread/sleep sleep))
                                  (inc i))
                                0
                                @(:value op))
                        (maybe-abort test)
                        (when (and (#{:poll :txn} (:f op))
                                   (not (:txn? test))
                                   (:subscribe (:sub-via test)))
                          (try (.commitSync consumer)
                               (catch RuntimeException e
                                 (assoc op :type :ok
                                        :error [:consumer-commit
                                                (.getMessage e)]))))
                        (assoc op :type :ok)))))))))))

  (teardown! [this test])

  (close! [this test]
    (rc/close! admin)
    (rc/close-producer! producer)
    (rc/close-consumer! consumer))

  client/Reusable
  (reusable? [this test]
             false))

(defn client
  "Constructs a fresh client for this workload."
  []
  (map->Client {:extant-topics (atom #{})}))

(defn op->max-offsets
  "Takes an operation and returns a map of keys to the highest offsets
  interacted with in that op."
  [{:keys [type f value]}]
  (case type
    (:info, :ok)
    (case f
      (:poll, :send, :txn)
      (->> value
           (map (fn [[f :as mop]]
                  (case f
                    :poll (->> (second mop)
                               (map-vals (fn [pairs]
                                           (->> pairs
                                                (map first)
                                                (remove nil?)
                                                (reduce max -1)))))
                    :send (let [[_ k v] mop]
                            (when (and (vector? v) (first v))
                              {k (first v)})))))
           (reduce (partial merge-with max)))

      nil)
    nil))

(defn workload
  "Constructs a workload (a map with a generator, client, checker, etc) given
  an options map."
  [opts]
  (let [workload (jepsen.tests.kafka/workload opts)]
    (assoc workload :client (client))))
