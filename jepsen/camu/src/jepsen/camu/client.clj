(ns jepsen.camu.client
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [clj-http.client :as http]
            [cheshire.core :as json])
  (:import (java.net ConnectException SocketTimeoutException)))

(def default-topic "jepsen-test")
(def http-timeout-ms 5000)
(def drain-timeout-ms 15000)

(defn base-url
  "Returns the base URL for a given node."
  [node]
  (str "http://" node ":8080"))

(defn create-topic!
  "Creates the test topic. Idempotent — ignores 409 Conflict."
  [node topic]
  (try
    (let [resp (http/post (str (base-url node) "/v1/topics")
                          {:content-type      :json
                           :body              (json/generate-string {:name       topic
                                                                     :partitions 4
                                                                     :retention  "24h"})
                           :socket-timeout    http-timeout-ms
                           :connect-timeout   http-timeout-ms
                           :throw-exceptions  false})]
      (when (not (#{200 201 409} (:status resp)))
        (warn "Create topic returned" (:status resp) "on" node)))
    (catch ConnectException _
      (warn "Could not connect to" node "to create topic"))
    (catch SocketTimeoutException _
      (warn "Timeout creating topic on" node))))

(defn produce!
  "Produces a message to the given topic. Returns {:partition N :offset M}
   or throws with a keyword status on failure."
  [node topic key value]
  (let [resp (http/post (str (base-url node) "/v1/topics/" topic "/messages")
                        {:content-type      :json
                         :body              (json/generate-string {:key   key
                                                                   :value value})
                         :socket-timeout    http-timeout-ms
                         :connect-timeout   http-timeout-ms
                         :throw-exceptions  false})
        status (:status resp)]
    (cond
      (= status 200) (let [body (json/parse-string (:body resp) true)
                           info (first (:offsets body))]
                       {:partition (:partition info) :offset (:offset info)})
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
   (let [resp (http/get (str (base-url node) "/v1/topics/" topic
                             "/partitions/" partition "/messages")
                        {:query-params     {:offset offset
                                            :limit  limit}
                         :socket-timeout   http-timeout-ms
                         :connect-timeout  http-timeout-ms
                         :throw-exceptions false})]
     (if (= 200 (:status resp))
       (let [body (json/parse-string (:body resp) true)]
         (vec (map (fn [m] {:offset    (:offset m)
                            :partition partition
                            :key       (:key m)
                            :value     (:value m)})
                   (:messages body))))
       []))))

(defn drain!
  "Drains all messages from a partition by paginating in batches of 1000
   (the server's max limit). Used for the final verification phase."
  [node topic partition offset]
  (loop [current-offset offset
         acc            []]
    (let [resp (http/get (str (base-url node) "/v1/topics/" topic
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
        acc))))

(defn commit-offsets!
  "Commits the consumer offset for the given topic and consumer-id."
  [node topic consumer-id partition offset]
  (http/post (str (base-url node) "/v1/topics/" topic
                  "/offsets/" consumer-id)
             {:content-type      :json
              :body              (json/generate-string {:offsets {(str partition) offset}})
              :socket-timeout    http-timeout-ms
              :connect-timeout   http-timeout-ms
              :throw-exceptions  false})
  :ok)

(defn get-offsets!
  "Fetches the committed consumer offset for the given topic and consumer-id."
  [node topic consumer-id]
  (let [resp (http/get (str (base-url node) "/v1/topics/" topic
                            "/offsets/" consumer-id)
                       {:socket-timeout    http-timeout-ms
                        :connect-timeout   http-timeout-ms
                        :throw-exceptions  false})]
    (when (= 200 (:status resp))
      (:offsets (json/parse-string (:body resp) true)))))

(defn wait-for-topic-ready!
  "Polls nodes until at least one can serve a produce request without 500.
   This ensures coordination has propagated topic initialization."
  [nodes topic timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [ready? (some (fn [n]
                           (try
                             (let [resp (http/post (str (base-url n) "/v1/topics/" topic "/messages")
                                                   {:content-type     :json
                                                    :body             (json/generate-string {:key "healthcheck" :value "ping"})
                                                    :socket-timeout   2000
                                                    :connect-timeout  2000
                                                    :throw-exceptions false})]
                               ;; 200 = produced, 421 = misdirected (but topic is initialized)
                               (#{200 421} (:status resp)))
                             (catch Exception _ false)))
                         nodes)]
        (cond
          ready?                                   (info "Topic ready on cluster")
          (> (System/currentTimeMillis) deadline)  (warn "Timed out waiting for topic readiness")
          :else                                    (do (Thread/sleep 2000) (recur)))))))

(defrecord CamuClient [node topic]
  client/Client
  (open! [this test node']
    (assoc this :node node'))

  (setup! [this test]
    ;; Create topic on first available node
    (create-topic! (first (:nodes test)) default-topic)
    ;; Wait for coordination to propagate topic to all nodes
    (wait-for-topic-ready! (:nodes test) default-topic 30000)
    this)

  (invoke! [this test op]
    (try
     (case (:f op)
       :produce
       (let [{:keys [key value]} (:value op)]
         ;; Try all nodes until one accepts (handles 421 misdirected)
         (loop [nodes (shuffle (:nodes test))]
           (if (empty? nodes)
             (assoc op :type :fail :error :no-node-available)
             (let [result (try (produce! (first nodes) default-topic key value)
                               (catch clojure.lang.ExceptionInfo e
                                 (if (#{:misdirected :not-ready} (:type (ex-data e)))
                                   ::retry
                                   (throw e))))]
               (if (= ::retry result)
                 (recur (rest nodes))
                 (assoc op :type :ok :value {:key       key
                                             :value     value
                                             :partition (:partition result)
                                             :offset    (:offset result)}))))))

       :consume
       (let [{:keys [partition offset]} (:value op)
             messages (consume! node default-topic partition (or offset 0))]
         ;; Advance the consume-offsets tracker so next consume continues forward
         (when-let [offsets-atom (:consume-offsets test)]
           (when (seq messages)
             (let [max-offset (apply max (map :offset messages))]
               (swap! offsets-atom update partition #(max (or % 0) (inc max-offset))))))
         (assoc op :type :ok :value {:partition partition :messages messages}))

       :drain
       (let [{:keys [partition offset]} (:value op)
             ;; Try all nodes for drain, skipping nodes that are down
             messages (loop [nodes (shuffle (:nodes test))]
                        (if (empty? nodes)
                          []
                          (let [msgs (try (drain! (first nodes) default-topic
                                                  partition (or offset 0))
                                          (catch ConnectException _ nil)
                                          (catch SocketTimeoutException _ nil))]
                            (if (seq msgs)
                              msgs
                              (recur (rest nodes))))))]
         (assoc op :type :ok :value {:partition partition :messages messages}))

       :commit-offsets
       (let [{:keys [consumer-id partition offset]} (:value op)
             result (commit-offsets! node default-topic consumer-id partition offset)]
         (assoc op :type :ok :value result))

       :get-offsets
       (let [{:keys [consumer-id]} (:value op)
             result (get-offsets! node default-topic consumer-id)]
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
           (assoc op :type :fail :error (str (.getMessage e))))))

     (catch Exception e
       (assoc op :type :info :error (str (.getMessage e))))))

  (teardown! [this test])

  (close! [this test]))

(defn client
  "Constructs a new CamuClient."
  []
  (->CamuClient nil default-topic))
