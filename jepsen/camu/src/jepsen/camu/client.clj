(ns jepsen.camu.client
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [clj-http.client :as http]
            [cheshire.core :as json])
  (:import (java.net ConnectException SocketTimeoutException)))

(def default-topic "jepsen-test")
(def http-timeout-ms 5000)
(def drain-timeout-ms 15000)

(def default-read-mode :leader)

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
           body (cond-> {:name       topic
                         :partitions 4
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

(defn produce!
  "Produces a message to the given topic. Returns {:partition N :offset M}
   or throws with a keyword status on failure."
  [node topic key value]
  (let [resp (http/post (str (normalize-base-url node) "/v1/topics/" topic "/messages")
                        {:content-type      :json
                         :body              (json/generate-string {:key   key
                                                                   :value value})
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
          (if (< (count msgs) 1000)
            ;; Last page — return all accumulated messages
            (into acc batch)
            ;; More pages — continue from next_offset
            (recur (:next_offset body) (into acc batch))))
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
    (info "Leader candidates for" topic "partition" partition ":" candidates)
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
    (info "Replica candidates for" topic "partition" partition ":" candidates)
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
  "Polls nodes by attempting a produce until one succeeds (200).
   This proves the full pipeline works: topic created, assignments published,
   partitions initialized, and for rf>1 topics, replicaState + followers ready."
  [nodes topic timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [ready? (some (fn [n]
                           (try
                             (let [resp (http/post (str (base-url n) "/v1/topics/" topic "/messages")
                                                   {:content-type     :json
                                                    :body             (json/generate-string
                                                                       {:key "readiness-probe" :value "ping"})
                                                    :socket-timeout   5000
                                                    :connect-timeout  2000
                                                    :throw-exceptions false})]
                               (info "Readiness probe on" n ":" (:status resp))
                               (= 200 (:status resp)))
                             (catch Exception _ false)))
                         nodes)]
        (cond
          ready?
          (info "Topic" topic "ready — produce succeeded")

          (> (System/currentTimeMillis) deadline)
          (warn "Timed out waiting for topic readiness")

          :else
          (do (Thread/sleep 3000)
              (recur)))))))

(defrecord CamuClient [node topic]
  client/Client
  (open! [this test node']
    (assoc this :node node'))

  (setup! [this test]
    (let [topic (topic-name this test)]
      ;; Topic names are unique per test run, so setup can be idempotent.
      (ensure-topic-created! (:nodes test) topic
                             (select-keys test [:replication-factor :min-insync-replicas])
                             60000)
    ;; All clients wait until a produce actually succeeds — proves the full replication
    ;; pipeline is working end-to-end (topic, assignments, replicaState, followers).
      (wait-for-topic-ready! (:nodes test) topic 60000))
    this)

  (invoke! [this test op]
    (try
     (case (:f op)
       :produce
       (let [{:keys [key value]} (:value op)
             topic             (topic-name this test)]
         ;; Try all nodes until one accepts (handles 421 misdirected)
         (loop [nodes (shuffle (:nodes test))]
           (if (empty? nodes)
             (assoc op :type :fail :error :no-node-available)
             (let [result (try (produce! (first nodes) topic key value)
                               (catch clojure.lang.ExceptionInfo e
                                 (if (#{:misdirected :not-ready} (:type (ex-data e)))
                                   ::retry
                                   (throw e))))]
               (if (= ::retry result)
                 (recur (rest nodes))
                 (assoc op :type :ok :value (merge {:key   key
                                                    :value value}
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
             deadline (+ (System/currentTimeMillis) (leader-read-deadline-ms :drain))
             messages (loop [nodes (read-candidates test topic partition)]
                        (if (empty? nodes)
                          (if (and (#{:leader :replica} (read-mode test))
                                   (< (System/currentTimeMillis) deadline))
                            (do (Thread/sleep 100)
                                (recur (read-candidates test topic partition)))
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
                                  (if (and (#{:leader :replica} (read-mode test))
                                           (< (System/currentTimeMillis) deadline))
                                    (do (Thread/sleep 100)
                                        (recur (read-candidates test topic partition)))
                                    (recur remaining))))
                              result))))]
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
