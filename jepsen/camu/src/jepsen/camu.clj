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
            [jepsen.camu.db :as db]))

(def partitions 4)

(defn camu-test
  "Constructs a Jepsen test map for camu."
  [opts]
  (let [faults  (:faults opts #{:kill})
        counter (atom 0)]
    (merge tests/noop-test
           opts
           {:name      "camu"
            :os        os/noop
            :db        (db/db)
            :client    (client/client)
            :nemesis   (nem/composed-nemesis faults)
            :checker   (checker/compose
                        {:no-data-loss        (camu-checker/no-data-loss-checker)
                         :offset-monotonicity (camu-checker/offset-monotonicity-checker)
                         :no-split-brain      (camu-checker/no-split-brain-checker)
                         :availability        (camu-checker/availability-checker)
                         :lease-fencing       (camu-checker/lease-fencing-checker)
                         :recovery-time       (camu-checker/recovery-time-checker)
                         :stats               (checker/stats)})
            :generator
            (let [produce  (fn [_ _]
                             (let [n (swap! counter inc)]
                               {:type  :invoke
                                :f     :produce
                                :value {:key   (str "k-" n)
                                        :value (str "v-" n)}}))
                  ;; Nemesis: cycle of start/sleep/stop/sleep
                  nem-gen  (->> (gen/mix
                                 (mapv (fn [fault]
                                         (gen/cycle
                                          [{:type :info :f fault :value :start}
                                           (gen/sleep 10)
                                           {:type :info :f fault :value :stop}
                                           (gen/sleep 5)]))
                                       faults)))]
              (gen/phases
               ;; Phase 1: clients produce + nemesis injects faults
               (->> (gen/any produce nem-gen)
                    (gen/time-limit (:time-limit opts 60)))
               ;; Phase 2: stop nemesis, let cluster recover
               (gen/log "Recovering...")
               (gen/nemesis {:type :info :f :kill :value :stop})
               (gen/sleep 15)
               ;; Phase 3: drain — each client reads one partition
               (gen/log "Draining partitions...")
               (gen/clients
                (gen/each-thread
                 (gen/once
                  (fn [_ _]
                    {:type  :invoke
                     :f     :consume
                     :value {:partition (rand-int partitions)
                             :offset   0}}))))))})))

(def cli-opts
  "Additional CLI options for camu tests."
  [[nil "--s3-endpoint URL" "MinIO/S3 endpoint"
    :default "http://minio:9000"]
   [nil "--camu-binary PATH" "Path to camu binary"
    :default "/jepsen/camu/camu"]
   [nil "--http-port PORT" "HTTP port for camu"
    :default 8080
    :parse-fn #(Integer/parseInt %)]
   [nil "--faults FAULTS" "Comma-separated fault types: kill,partition,pause"
    :default #{:kill}
    :parse-fn (fn [s] (set (map keyword (clojure.string/split s #","))))]])

(defn -main
  "Entry point for the Jepsen CLI."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn  camu-test
                                 :opt-spec cli-opts})
          (cli/serve-cmd))
   args))
