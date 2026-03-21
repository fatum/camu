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

(defn produce-gen
  "Returns a generator that produces messages with unique sequential keys."
  [counter]
  (fn [_ _]
    (let [n (swap! counter inc)]
      {:type  :invoke
       :f     :produce
       :value {:key   (str "k-" n)
               :value (str "v-" n)}})))

(defn consume-gen
  "Returns a generator that consumes from a random partition at offset 0."
  []
  (fn [_ _]
    {:type  :invoke
     :f     :consume
     :value {:partition (rand-int partitions)
              :offset   0}}))

(defn mixed-workload-gen
  "Returns a mixed generator: ~70% produce, ~30% consume."
  [counter]
  (gen/mix [(produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (produce-gen counter)
            (consume-gen)
            (consume-gen)
            (consume-gen)]))

(defn drain-gen
  "Returns a generator that drains all partitions (0..3).
   Each thread drains partition = (mod process partitions)."
  []
  (gen/each-thread
   (gen/once
    (fn [test ctx]
      {:type  :invoke
       :f     :drain
       :value {:partition (mod (:process ctx) partitions)
               :offset   0}}))))

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
                         :total-order         (camu-checker/total-order-checker)
                         :availability        (camu-checker/availability-checker)
                         :lease-fencing       (camu-checker/lease-fencing-checker)
                         :recovery-time       (camu-checker/recovery-time-checker)
                         :stats               (checker/stats)})
            :generator
            (let [;; Mixed workload: 70% produce, 30% consume
                  workload (mixed-workload-gen counter)
                  ;; Nemesis: cycle of start/sleep/stop/sleep per fault
                  nem-gen  (->> (gen/mix
                                 (mapv (fn [fault]
                                         (gen/cycle
                                          [{:type :info :f fault :value :start}
                                           (gen/sleep 10)
                                           {:type :info :f fault :value :stop}
                                           (gen/sleep 5)]))
                                       faults)))]
              (gen/phases
               ;; Phase 1: clients produce+consume, nemesis injects faults
               (->> (gen/any workload nem-gen)
                    (gen/time-limit (:time-limit opts 300)))
               ;; Phase 2: stop all nemeses, let cluster recover
               (gen/log "Stopping all nemeses...")
               (apply gen/phases
                      (for [fault faults]
                        (gen/nemesis
                         (gen/once {:type :info :f fault :value :stop}))))
               (gen/log "Recovering — waiting 15s for cluster stabilization...")
               (gen/sleep 15)
               ;; Phase 3: drain ALL partitions from ALL nodes
               (gen/log "Draining all partitions for verification...")
               (gen/clients (drain-gen))))})))

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
