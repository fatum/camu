(ns jepsen.camu.client
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [clj-http.client :as http]
            [cheshire.core :as json])
  (:import (java.net ConnectException SocketTimeoutException)
           (java.time Instant)
           (java.nio.charset StandardCharsets)))

(def default-topic "jepsen-test")
(def http-timeout-ms 5000)
(def drain-timeout-ms 15000)

(def default-read-mode :leader)
(def minio-bucket "camu-data")

(def quiet-client-logs?
  (= "1" (System/getenv "CAMU_QUIET_CLIENT_LOGS")))

(defmacro maybe-info
  [& args]
  (when-not quiet-client-logs?
    `(info ~@args)))

(defn topic-name
  [this test]
  (or (:topic this) (:topic test) default-topic))

(defn base-url
  "Returns the base URL for a given node."
  [node]
  (str "http://" node ":8080"))

(defn normalize-base-url
  [node-or-url]
  (if (str/starts-with? node-or-url "http://")
    node-or-url
    (base-url node-or-url)))

(defn create-topic!
  "Creates the test topic. Idempotent — ignores 409 Conflict.
   Accepts optional replication-factor and min-insync-replicas from test opts."
  ([node topic]
   (create-topic! node topic {}))
  ([node topic opts]
   (try
     (let [rf  (get opts :replication-factor 1)
           mir (get opts :min-insync-replicas 1)
           partitions (get opts :num-partitions 4)
           body (cond-> {:name       topic
                         :partitions partitions
                         :retention  "24h"}
                  (> rf 1) (assoc :replication_factor  rf
                                  :min_insync_replicas mir))
           resp (http/post (str (base-url node) "/v1/topics")
                           {:content-type      :json
                            :body              (json/generate-string body)
                            :socket-timeout    http-timeout-ms
                            :connect-timeout   http-timeout-ms
                            :throw-exceptions  false})]
       (if (#{200 201 409} (:status resp))
         true
         (do (warn "Create topic returned" (:status resp) "on" node)
             false)))
     (catch ConnectException _
       (warn "Could not connect to" node "to create topic")
       false)
     (catch SocketTimeoutException _
       (warn "Timeout creating topic on" node)
       false))))

(defn ensure-topic-created!
  "Retries topic creation across the cluster until one node accepts it."
  [nodes topic opts timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [created? (some #(create-topic! % topic opts) (shuffle nodes))]
        (cond
          created?
          (info "Topic" topic "created or already present")

          (> (System/currentTimeMillis) deadline)
          (warn "Timed out creating topic" topic)

          :else
          (do (Thread/sleep 1000)
              (recur)))))))

(defn route-partition
  "Matches camu's producer.Router: FNV-32a over UTF-8 bytes."
  [key num-partitions]
  (let [data (.getBytes ^String key StandardCharsets/UTF_8)
        hash (reduce (fn [h b]
                       (let [h (bit-xor h (bit-and (int b) 0xff))]
                         (bit-and (* h 16777619) 0xffffffff)))
                     2166136261
                     data)]
    (int (mod hash num-partitions))))

(defn produce!
  "Produces a message to a specific partition. Returns {:partition N :offset M}
   or throws with a keyword status on failure."
  [node topic partition key value]
  (let [resp (http/post (str (normalize-base-url node)
                             "/v1/topics/" topic
                             "/partitions/" partition "/messages")
                        {:content-type      :json
                         :body              (json/generate-string [{:key key :value value}])
                         :socket-timeout    http-timeout-ms
                         :connect-timeout   http-timeout-ms
                         :throw-exceptions  false})
        status (:status resp)]
    (cond
      (= status 200) (let [body   (json/parse-string (:body resp) true)
                           pinfo  (first (:offsets body))
                           headers (:headers resp)
                           epoch  (when-let [e (get headers "x-leader-epoch")]
                                    (Long/parseLong e))]
                       (cond-> {:partition (:partition pinfo)
                                :offset   (:offset pinfo)
                                :node     (or (get headers "x-camu-instance-id") node)}
                         epoch (assoc :leader-epoch epoch)))
      (= status 421) (throw (ex-info "misdirected" {:type :misdirected}))
      (= status 500) (throw (ex-info "not-ready" {:type :not-ready}))
      (= status 503) (throw (ex-info "backpressure" {:type :backpressure}))
      :else          (throw (ex-info (str "produce failed: " status) {:type :error :status status})))))

(defn init-producer!
  "Initializes a producer on the given node. Returns the producer ID."
  [node]
  (let [resp (http/post (str (normalize-base-url node) "/v1/producers/init")
                        {:content-type      :json
                         :socket-timeout    http-timeout-ms
                         :connect-timeout   http-timeout-ms
                         :throw-exceptions  false})]
    (if (= 201 (:status resp))
      (:producer_id (json/parse-string (:body resp) true))
      (throw (ex-info (str "init-producer failed: " (:status resp))
                      {:type :init-producer-failed :status (:status resp)})))))

(defn idempotent-produce!
  "Produces a message with idempotency fields to a specific partition.
   Returns a map with :partition/:offset on success, or {:duplicate true}
   if the server detected a duplicate (and confirmed replication)."
  [node topic partition producer-id sequence key value]
  (let [resp (http/post (str (normalize-base-url node) "/v1/topics/" topic
                             "/partitions/" partition "/messages")
                        {:content-type      :json
                         :body              (json/generate-string
                                             {:producer_id producer-id
                                              :sequence    sequence
                                              :messages    [{:key key :value value}]})
                         :socket-timeout    http-timeout-ms
                         :connect-timeout   http-timeout-ms
                         :throw-exceptions  false})
        status (:status resp)]
    (cond
      (= status 200) (let [body    (json/parse-string (:body resp) true)
                           headers (:headers resp)]
                       (if (:duplicate body)
                         {:duplicate true
                          :node      (or (get headers "x-camu-instance-id") node)}
                         (let [pinfo (first (:offsets body))
                               epoch (when-let [e (get headers "x-leader-epoch")]
                                       (Long/parseLong e))]
                           (cond-> {:partition (:partition pinfo)
                                    :offset   (:offset pinfo)
                                    :node     (or (get headers "x-camu-instance-id") node)}
                             epoch (assoc :leader-epoch epoch)))))
      (= status 421) (throw (ex-info "misdirected" {:type :misdirected}))
      (= status 422) (throw (ex-info "sequence error" {:type :sequence-error}))
      (= status 503) (throw (ex-info "backpressure" {:type :backpressure}))
      :else          (throw (ex-info (str "idempotent produce failed: " status) {:type :error :status status})))))

(defn consume!
  "Consumes messages from the given topic and partition. Returns a vec of
   plain serializable message maps."
  ([node topic partition offset]
   (consume! node topic partition offset 1000))
  ([node topic partition offset limit]
   (let [resp (http/get (str (normalize-base-url node) "/v1/topics/" topic
                             "/partitions/" partition "/messages")
                        {:query-params     {:offset offset
                                            :limit  limit}
                         :socket-timeout   http-timeout-ms
                         :connect-timeout  http-timeout-ms
                         :throw-exceptions false})]
     (if (= 200 (:status resp))
       (let [body (json/parse-string (:body resp) true)
             hw   (when-let [h (get-in resp [:headers "x-high-watermark"])]
                    (Long/parseLong h))
             msgs (vec (map (fn [m] {:offset    (:offset m)
                                     :partition partition
                                     :key       (:key m)
                                     :value     (:value m)})
                            (:messages body)))]
         (cond-> {:messages msgs}
           hw (assoc :high-watermark hw)))
       (throw (ex-info (str "consume failed: " (:status resp))
                       {:type :consume-failed
                        :status (:status resp)
                        :partition partition
                        :offset offset}))))))

(defn drain!
  "Drains all messages from a partition by paginating in batches of 1000
   (the server's max limit). Used for the final verification phase."
  [node topic partition offset]
  (loop [current-offset offset
         acc            []]
    (let [resp (http/get (str (normalize-base-url node) "/v1/topics/" topic
                              "/partitions/" partition "/messages")
                         {:query-params     {:offset current-offset
                                             :limit  1000}
                          :socket-timeout   drain-timeout-ms
                          :connect-timeout  drain-timeout-ms
                          :throw-exceptions false})]
      (if (= 200 (:status resp))
        (let [body    (json/parse-string (:body resp) true)
              msgs    (:messages body)
              batch   (mapv (fn [m] {:offset    (:offset m)
                                     :partition partition
                                     :key       (:key m)
                                     :value     (:value m)})
                            msgs)]
          (let [next-off (:next_offset body)]
            (if (or (nil? next-off) (empty? msgs))
              ;; No next_offset or empty page — done
              (into acc batch)
              ;; More pages — continue from next_offset
              (recur next-off (into acc batch)))))
        (throw (ex-info (str "drain failed: " (:status resp))
                        {:type :drain-failed
                         :status (:status resp)
                         :partition partition
                         :offset current-offset}))))))

(defn get-routing!
  "Queries the routing endpoint for a topic. Returns the parsed routing map
   or nil on failure."
  [node topic]
  (try
    (let [resp (http/get (str (normalize-base-url node) "/v1/topics/" topic "/routing")
                         {:socket-timeout    http-timeout-ms
                          :connect-timeout   http-timeout-ms
                          :throw-exceptions  false})]
      (when (= 200 (:status resp))
        (json/parse-string (:body resp) true)))
    (catch Exception _ nil)))

(defn read-mode
  [test]
  (or (:read-mode test) default-read-mode))

(defn routing-partition
  [routing partition]
  (or (get-in routing [:partitions (str partition)])
      (get-in routing [:partitions (keyword (str partition))])))

(defn routing-ready?
  "Returns true when routing exposes every partition with a leader address.
   For replicated topics, also requires the advertised replica list to reach
   the requested replication factor."
  [routing num-partitions replication-factor]
  (boolean
   (and routing
        (= num-partitions (count (:partitions routing)))
        (every?
         (fn [partition]
           (when-let [entry (routing-partition routing partition)]
             (and (:address entry)
                  (or (<= replication-factor 1)
                      (= replication-factor (count (or (:replicas entry) [])))))))
         (range num-partitions)))))

(defn- s3-key
  [suffix]
  (str "local/" minio-bucket "/" suffix))

(defn- mc
  [& args]
  (let [base ["mc" "--config-dir" "/tmp/.mc-jepsen"]]
    (apply sh/sh (concat base args))))

(defn- mc-init!
  []
  (mc "alias" "set" "local" "http://minio:9000" "minioadmin" "minioadmin"))

(defn- mc-find-lines
  [prefix]
  (let [{:keys [exit out]} (mc "find" (s3-key prefix) "--name" "*.json")]
    (if (zero? exit)
      (->> (str/split-lines out)
           (remove str/blank?))
      [])))

(defn- leader-lease-ready?
  []
  (let [{:keys [exit out]} (mc "cat" (s3-key "_coordination/leader.json"))]
    (when (zero? exit)
      (let [lease      (json/parse-string out true)
            instance   (:instance_id lease)
            expires-at (:expires_at lease)]
        (and (not (str/blank? instance))
             (not (str/blank? expires-at))
             (.isAfter (Instant/parse expires-at) (Instant/now)))))))

(defn s3-coordination-ready?
  "Returns true when S3 coordination state exists for every partition:
   the cluster leader lease exists, assignments cover all partitions, and
   epoch/ISR objects exist per partition."
  [topic num-partitions replication-factor]
  (let [{:keys [exit out]} (mc "cat" (s3-key (str "_coordination/assignments/" topic ".json")))]
    (when (zero? exit)
      (let [assignment (json/parse-string out true)
            partitions (:partitions assignment)
            epoch-files (mc-find-lines (str "_coordination/epochs/" topic))
            isr-files   (mc-find-lines (str "_coordination/isr/" topic))]
        (and (leader-lease-ready?)
             (= num-partitions (count partitions))
             (= num-partitions (count epoch-files))
             (= num-partitions (count isr-files))
             (every?
              (fn [partition]
                (when-let [entry (routing-partition assignment partition)]
                  (and (:leader entry)
                       (= replication-factor (count (or (:replicas entry) []))))))
              (range num-partitions)))))))

(defn leader-candidates
  "Resolves candidate owners for a partition by querying routing from the cluster.
   Multiple nodes may briefly disagree during reassignment, so we try each distinct
   reported owner address in order."
  [nodes topic partition]
  (let [candidates (->> nodes
                        shuffle
                        (reduce (fn [acc node]
                                  (if-let [entry (some-> (get-routing! node topic)
                                                         (routing-partition partition))]
                                    (let [address (:address entry)]
                                      (if (or (nil? address) (some #{address} acc))
                                        acc
                                        (conj acc address)))
                                    acc))
                                []))]
    (maybe-info "Leader candidates for" topic "partition" partition ":" candidates)
    candidates))

(defn replica-candidates
  "Resolves non-leader replica candidates for a partition. Multiple nodes may
   briefly disagree during reassignment, so we try each distinct replica address
   reported by routing in order."
  [nodes topic partition]
  (let [candidates (->> nodes
                        shuffle
                        (reduce (fn [acc node]
                                  (if-let [entry (some-> (get-routing! node topic)
                                                         (routing-partition partition))]
                                    (let [leader-address (:address entry)
                                          replicas       (or (:replicas entry) [])
                                          addresses      (->> replicas
                                                              (keep :address)
                                                              (remove nil?)
                                                              (remove #{leader-address}))]
                                      (reduce (fn [inner address]
                                                (if (some #{address} inner)
                                                  inner
                                                  (conj inner address)))
                                              acc
                                              addresses))
                                    acc))
                                []))]
    (maybe-info "Replica candidates for" topic "partition" partition ":" candidates)
    candidates))

(defn read-candidates
  "Returns the node candidates to use for read-style operations.
   `:leader` resolves the current owner and reads only from that node.
   `:replica` resolves known followers and reads only from them.
   `:any` preserves the previous best-effort replica read behavior."
  [test topic partition]
  (case (read-mode test)
    :any
    (shuffle (:nodes test))

    :leader
    (leader-candidates (:nodes test) topic partition)

    :replica
    (replica-candidates (:nodes test) topic partition)

    []))

(defn leader-read-deadline-ms
  [f]
  (case f
    :drain 20000
    :consume 8000
    8000))

(defn drain-with-candidates
  "Drains a partition by trying nodes returned by candidate-fn. Retries within
   deadline. candidate-fn is called as (candidate-fn) and must return a seq of
   addresses. When retry-guard-fn is provided it must return true for deadline
   retries to proceed; otherwise retries always proceed within the deadline."
  [topic partition offset candidate-fn retry-guard-fn]
  (let [deadline (+ (System/currentTimeMillis) (leader-read-deadline-ms :drain))]
    (loop [nodes (candidate-fn)]
      (if (empty? nodes)
        (if (and (if retry-guard-fn (retry-guard-fn) true)
                 (< (System/currentTimeMillis) deadline))
          (do (Thread/sleep 100)
              (recur (candidate-fn)))
          (throw (ex-info "drain failed on all nodes"
                          {:type :drain-failed
                           :status :no-node-available
                           :partition partition
                           :offset (or offset 0)})))
        (let [result (try
                       (drain! (first nodes) topic partition (or offset 0))
                       (catch ConnectException _ ::retry)
                       (catch SocketTimeoutException _ ::retry)
                       (catch clojure.lang.ExceptionInfo e
                         (if (= :drain-failed (:type (ex-data e)))
                           (if (= 421 (:status (ex-data e)))
                             ::retry
                             (throw e))
                           (throw e))))]
          (if (= ::retry result)
            (let [remaining (rest nodes)]
              (if (seq remaining)
                (recur remaining)
                (if (and (if retry-guard-fn (retry-guard-fn) true)
                         (< (System/currentTimeMillis) deadline))
                  (do (Thread/sleep 100)
                      (recur (candidate-fn)))
                  (throw (ex-info "drain failed on all nodes (deadline expired)"
                                  {:type :drain-failed
                                   :status :no-node-available
                                   :partition partition
                                   :offset (or offset 0)})))))
            result))))))

(defn commit-offsets!
  "Commits the consumer offset for the given topic and consumer-id."
  [node topic consumer-id partition offset]
  (let [resp (http/post (str (base-url node) "/v1/topics/" topic
                             "/offsets/" consumer-id)
                        {:content-type      :json
                         :body              (json/generate-string {:offsets {(str partition) offset}})
                         :socket-timeout    http-timeout-ms
                         :connect-timeout   http-timeout-ms
                         :throw-exceptions  false})]
    (if (= 200 (:status resp))
      :ok
      (throw (ex-info (str "commit offsets failed: " (:status resp))
                      {:type :offset-commit-failed
                       :status (:status resp)
                       :partition partition
                       :offset offset})))))

(defn get-offsets!
  "Fetches the committed consumer offset for the given topic and consumer-id."
  [node topic consumer-id]
  (let [resp (http/get (str (base-url node) "/v1/topics/" topic
                            "/offsets/" consumer-id)
                       {:socket-timeout    http-timeout-ms
                        :connect-timeout   http-timeout-ms
                        :throw-exceptions  false})]
    (if (= 200 (:status resp))
      (:offsets (json/parse-string (:body resp) true))
      (throw (ex-info (str "get offsets failed: " (:status resp))
                      {:type :offset-fetch-failed
                       :status (:status resp)})))))

(defn wait-for-topic-ready!
  "Waits until at least one node reports complete routing for all partitions,
   then verifies the produce path works with a successful probe."
  [nodes topic num-partitions replication-factor timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [routing-ready (some (fn [n]
                                  (try
                                    (let [routing (get-routing! n topic)
                                          ready?  (routing-ready? routing num-partitions replication-factor)]
                                      (when routing
                                        (maybe-info "Routing readiness on" n ":" ready?
                                                    "partitions" (count (:partitions routing))))
                                      ready?)
                                    (catch Exception _ false)))
                                nodes)
            _ (mc-init!)
            s3-ready (s3-coordination-ready? topic num-partitions replication-factor)
            _ (maybe-info "S3 coordination readiness for" topic ":" s3-ready)
            produce-ready (and routing-ready
                               s3-ready
                               (every?
                                (fn [partition]
                                  (let [key (str "readiness-probe-" partition)]
                                    (boolean
                                     (some (fn [n]
                                             (try
                                               (let [result (produce! n topic partition key "ping")]
                                                 (maybe-info "Readiness probe on" n "partition" partition ":" result)
                                                 true)
                                               (catch clojure.lang.ExceptionInfo e
                                                 (maybe-info "Readiness probe on" n "partition" partition ":" (:type (ex-data e)))
                                                 (case (:type (ex-data e))
                                                   :misdirected false
                                                   :backpressure false
                                                   :not-ready false
                                                   :error false
                                                   false))
                                               (catch Exception _
                                                 false)))
                                           nodes))))
                                (range num-partitions)))]
        (cond
          produce-ready
          (info "Topic" topic "ready — produce succeeded")

          (> (System/currentTimeMillis) deadline)
          (warn "Timed out waiting for topic readiness")

          :else
          (do (Thread/sleep 3000)
              (recur)))))))

(defrecord CamuClient [node topic]
  client/Client
  (open! [this test node']
    (let [this' (assoc this :node node')]
      (if (= :idempotent (:workload test))
        ;; Each client thread gets its own producer ID and per-partition sequence counters.
        (let [pid (loop [nodes (shuffle (:nodes test))]
                    (if (empty? nodes)
                      (throw (ex-info "cannot init producer" {:type :init-failed}))
                      (let [result (try (init-producer! (first nodes))
                                       (catch Exception _ nil))]
                        (or result (recur (rest nodes))))))
              num-partitions (get test :num-partitions 4)
              ;; One sequence atom per partition.
              seqs (into {} (map (fn [p] [p (atom 0)]) (range num-partitions)))]
          (assoc this' :producer-id pid :sequences seqs :num-partitions num-partitions))
        this')))

  (setup! [this test]
    (let [topic (topic-name this test)]
      ;; Topic names are unique per test run, so setup can be idempotent.
      (ensure-topic-created! (:nodes test) topic
                             (select-keys test [:replication-factor :min-insync-replicas :num-partitions])
                             60000)
      ;; All clients wait until routing exposes every partition and a probe produce
      ;; succeeds, so large-partition runs do not start against partial assignments.
      (wait-for-topic-ready! (:nodes test)
                             topic
                             (get test :num-partitions 4)
                             (get test :replication-factor 1)
                             60000))
    this)

  (invoke! [this test op]
    (try
     (case (:f op)
       :idempotent-produce
       (let [{:keys [key value]} (:value op)
             topic       (topic-name this test)
             producer-id (:producer-id this)
             ;; Hash key to a partition using the same FNV-32a routing as camu.
             partition   (route-partition key (:num-partitions this))
             seq-atom    (get (:sequences this) partition)
             seq-val     @seq-atom]
         ;; Any node can proxy to the partition leader — just pick one.
         ;; Only retry on connection refused (node down) with a different node.
         (loop [nodes (shuffle (:nodes test))]
           (if (empty? nodes)
             ;; All nodes failed — but a node may have committed before the
             ;; connection dropped. Advance the sequence to avoid reusing this
             ;; slot with a different key (which would be silently deduped).
             (do (swap! seq-atom + 1)
                 (assoc op :type :info :error :no-node-available))
             (let [result (try (idempotent-produce!
                                 (first nodes) topic partition
                                 producer-id seq-val key value)
                               (catch ConnectException _ ::retry)
                               (catch SocketTimeoutException _ ::retry)
                               (catch clojure.lang.ExceptionInfo e
                                 (case (:type (ex-data e))
                                   :misdirected ::retry
                                   :backpressure ::retry
                                   :not-ready ::retry
                                   :error ::retry
                                   :sequence-error (throw e)
                                   (throw e))))]
               (if (= ::retry result)
                 (recur (rest nodes))
                 (do
                   ;; Advance this partition's sequence by batch size (1 message).
                   (swap! seq-atom + 1)
                   (assoc op :type :ok
                          :value (merge {:key key :value value
                                         :producer-id producer-id
                                         :sequence seq-val
                                         :partition partition}
                                        result))))))))

       :produce
       (let [{:keys [key value]} (:value op)
             topic             (topic-name this test)
             partition         (route-partition key (get test :num-partitions 4))]
         ;; Use only the partition-specific produce endpoint and try all nodes
         ;; until the current leader accepts the write.
         (loop [nodes (shuffle (:nodes test))
                had-timeout? false]
           (if (empty? nodes)
             ;; If any node timed out, the write may have committed — use :info.
             ;; Only use :fail when all nodes refused the connection outright.
             (assoc op :type (if had-timeout? :info :fail)
                    :error :no-node-available)
             (let [result (try (produce! (first nodes) topic partition key value)
                               (catch ConnectException _ ::retry)
                               (catch SocketTimeoutException _ ::timeout-retry)
                               (catch clojure.lang.ExceptionInfo e
                                 (if (#{:misdirected :not-ready :backpressure :error} (:type (ex-data e)))
                                   ::retry
                                   (throw e))))]
               (case result
                 ::retry         (recur (rest nodes) had-timeout?)
                 ::timeout-retry (recur (rest nodes) true)
                 (assoc op :type :ok :value (merge {:key   key
                                                    :value value
                                                    :partition partition}
                                                   result)))))))

       :consume
       (let [{:keys [partition offset]} (:value op)
             topic    (topic-name this test)
             deadline (+ (System/currentTimeMillis) (leader-read-deadline-ms :consume))
             result   (loop [nodes (read-candidates test topic partition)]
                        (if (empty? nodes)
                          (if (and (#{:leader :replica} (read-mode test))
                                   (< (System/currentTimeMillis) deadline))
                            (do (Thread/sleep 100)
                                (recur (read-candidates test topic partition)))
                            (throw (ex-info "consume failed on all nodes"
                                            {:type :consume-failed
                                             :status :no-node-available
                                             :partition partition
                                             :offset (or offset 0)})))
                          (let [result (try
                                         (consume! (first nodes) topic partition (or offset 0))
                                         (catch ConnectException _ ::retry)
                                         (catch SocketTimeoutException _ ::retry)
                                         (catch clojure.lang.ExceptionInfo e
                                           (if (= :consume-failed (:type (ex-data e)))
                                           (if (= 421 (:status (ex-data e)))
                                             ::retry
                                             (throw e))
                                           (throw e))))]
                            (if (= ::retry result)
                              (let [remaining (rest nodes)]
                                (if (seq remaining)
                                  (recur remaining)
                                  (if (and (#{:leader :replica} (read-mode test))
                                           (< (System/currentTimeMillis) deadline))
                                    (do (Thread/sleep 100)
                                        (recur (read-candidates test topic partition)))
                                    (recur remaining))))
                              result))))
             messages (:messages result)
             hw       (:high-watermark result)]
         ;; Advance the consume-offsets tracker so next consume continues forward
         (when-let [offsets-atom (:consume-offsets test)]
           (when (seq messages)
             (let [max-offset (apply max (map :offset messages))]
               (swap! offsets-atom update partition #(max (or % 0) (inc max-offset))))))
         (assoc op :type :ok :value (cond-> {:partition partition :messages messages}
                                      hw (assoc :high-watermark hw))))

       :drain
       (let [{:keys [partition offset]} (:value op)
             topic    (topic-name this test)
             ;; Drain always reads from the leader for authoritative HW.
             messages (drain-with-candidates
                       topic partition offset
                       #(leader-candidates (:nodes test) topic partition)
                       #(#{:leader :replica} (read-mode test)))]
         (assoc op :type :ok :value {:partition partition :messages messages}))

       :replica-drain
       (let [{:keys [partition offset]} (:value op)
             topic    (topic-name this test)
             messages (drain-with-candidates
                       topic partition offset
                       #(replica-candidates (:nodes test) topic partition)
                       nil)]
         (assoc op :type :ok :value {:partition partition :messages messages}))

       :commit-offsets
       (let [{:keys [consumer-id partition offset]} (:value op)
             topic  (topic-name this test)
             result (commit-offsets! node topic consumer-id partition offset)]
         (assoc op :type :ok :value result))

       :get-offsets
       (let [{:keys [consumer-id]} (:value op)
             topic  (topic-name this test)
             result (get-offsets! node topic consumer-id)]
         (assoc op :type :ok :value result)))

     (catch ConnectException _
       (assoc op :type :fail :error :connection-refused))

     (catch SocketTimeoutException _
       (assoc op :type :info :error :timeout))

     (catch clojure.lang.ExceptionInfo e
       (let [t (:type (ex-data e))]
         (case t
           :misdirected        (assoc op :type :fail :error :misdirected)
           :not-ready          (assoc op :type :fail :error :not-ready)
           :backpressure       (assoc op :type :fail :error :service-unavailable)
           :sequence-error     (assoc op :type :fail :error :sequence-error)
           :consume-failed     (assoc op :type :fail :error [:consume-failed (:status (ex-data e))])
           :drain-failed       (assoc op :type :fail :error [:drain-failed (:status (ex-data e))])
           :offset-commit-failed (assoc op :type :fail :error [:offset-commit-failed (:status (ex-data e))])
           :offset-fetch-failed  (assoc op :type :fail :error [:offset-fetch-failed (:status (ex-data e))])
           (assoc op :type :fail :error (str (.getMessage e))))))

     (catch Exception e
       (assoc op :type :info :error (str (.getMessage e))))))

  (teardown! [this test])

  (close! [this test]))

(defn client
  "Constructs a new CamuClient."
  []
  (->CamuClient nil nil))
