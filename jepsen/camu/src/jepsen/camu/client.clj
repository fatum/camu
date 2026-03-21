(ns jepsen.camu.client
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [slingshot.slingshot :refer [try+]])
  (:import (java.net ConnectException SocketTimeoutException)))

(def default-topic "jepsen-test")
(def http-timeout-ms 5000)

(defn base-url
  "Returns the base URL for a given node."
  [node]
  (str "http://" node ":8080"))

(defn create-topic!
  "Creates the test topic. Idempotent — ignores 409 Conflict."
  [node topic]
  (try+
   (http/post (str (base-url node) "/v1/topics")
              {:content-type  :json
               :body          (json/generate-string {:name       topic
                                                     :partitions 4
                                                     :retention  "24h"})
               :socket-timeout  http-timeout-ms
               :connect-timeout http-timeout-ms})
   (catch [:status 409] _
     (info "Topic" topic "already exists"))
   (catch ConnectException _
     (warn "Could not connect to" node "to create topic"))
   (catch SocketTimeoutException _
     (warn "Timeout creating topic on" node))))

(defn produce!
  "Produces a message to the given topic. Returns the assigned offset on
   success, or throws on error."
  [node topic key value]
  (let [resp (http/post (str (base-url node) "/v1/topics/" topic "/messages")
                        {:content-type    :json
                         :body            (json/generate-string {:key   key
                                                                 :value value})
                         :socket-timeout  http-timeout-ms
                         :connect-timeout http-timeout-ms
                         :as              :json})]
    (first (get-in resp [:body :offsets]))))

(defn consume!
  "Consumes messages from the given topic and partition, starting at the
   given offset. Returns a seq of message maps."
  [node topic partition offset]
  (let [resp (http/get (str (base-url node) "/v1/topics/" topic
                            "/partitions/" partition "/messages")
                       {:query-params    {:offset offset
                                          :limit  1000}
                        :socket-timeout  http-timeout-ms
                        :connect-timeout http-timeout-ms
                        :as              :json})]
    (get-in resp [:body :messages] [])))

(defn commit-offsets!
  "Commits the consumer offset for the given topic and consumer-id.
   POST /v1/topics/{topic}/offsets/{consumer_id}
   Used to verify offset durability across faults."
  [node topic consumer-id partition offset]
  (let [resp (http/post (str (base-url node) "/v1/topics/" topic
                             "/offsets/" consumer-id)
                        {:content-type    :json
                         :body            (json/generate-string {:offsets {(str partition) offset}})
                         :socket-timeout  http-timeout-ms
                         :connect-timeout http-timeout-ms
                         :as              :json})]
    (get-in resp [:body])))

(defn get-offsets!
  "Fetches the committed consumer offset for the given topic and consumer-id.
   GET /v1/topics/{topic}/offsets/{consumer_id}
   Used to verify committed offsets survive instance restarts."
  [node topic consumer-id]
  (let [resp (http/get (str (base-url node) "/v1/topics/" topic
                            "/offsets/" consumer-id)
                       {:socket-timeout  http-timeout-ms
                        :connect-timeout http-timeout-ms
                        :as              :json})]
    (get-in resp [:body])))

(defrecord CamuClient [node topic]
  client/Client
  (open! [this test node']
    (assoc this :node node'))

  (setup! [this test]
    (create-topic! node default-topic)
    this)

  (invoke! [this test op]
    (try+
     (case (:f op)
       :produce
       (let [{:keys [key value]} (:value op)
             result (produce! node default-topic key value)]
         (assoc op :type :ok :value {:key       key
                                     :value     value
                                     :partition (:partition result)
                                     :offset    (:offset result)}))

       :consume
       (let [{:keys [partition offset]} (:value op)
             messages (consume! node default-topic partition (or offset 0))]
         (assoc op :type :ok :value messages))

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

     (catch [:status 500] {:keys [body]}
       (assoc op :type :fail :error [:server-error body]))

     (catch [:status 503] _
       (assoc op :type :fail :error :service-unavailable))

     (catch Exception e
       (assoc op :type :info :error (.getMessage e)))))

  (teardown! [this test])

  (close! [this test]))

(defn client
  "Constructs a new CamuClient."
  []
  (->CamuClient nil default-topic))
