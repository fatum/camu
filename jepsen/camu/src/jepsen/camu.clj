(ns jepsen.camu
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.camu.client :as client]
            [jepsen.camu.nemesis :as nem]
            [jepsen.camu.checker :as chk]
            [jepsen.camu.db :as db]))

(def partitions 4)

(defn produce-generator
  "Generates unique produce operations."
  []
  (let [counter (atom 0)]
    (reify gen/Generator
      (op [_ test context]
        (let [n (swap! counter inc)]
          {:type  :invoke
           :f     :produce
           :value {:key   (str "k-" n)
                   :value (str "v-" n)}})))))

(defn drain-generator
  "Generates consume operations for all partitions to drain remaining
   messages after the test completes."
  []
  (->> (range partitions)
       (map (fn [p]
              {:type  :invoke
               :f     :consume
               :value {:partition p :offset 0}}))
       gen/each-thread))

(defn camu-test
  "Constructs a Jepsen test map for camu."
  [opts]
  (merge tests/noop-test
         opts
         {:name      "camu"
          :os        jepsen.os/noop
          :db        (db/db)
          :client    (client/client)
          :nemesis   (nem/composed-nemesis)
          :checker   (chk/combined-checker)
          :generator (gen/phases
                      ;; Phase 1: produce under faults
                      (->> (produce-generator)
                           (gen/stagger 1/10)
                           (gen/nemesis (nem/nemesis-generator
                                        (:time-limit opts 120)))
                           (gen/time-limit (:time-limit opts 120)))
                      ;; Phase 2: recovery (nemesis already stopped by
                      ;; nemesis-generator, just wait)
                      (gen/sleep 15)
                      ;; Phase 3: drain all partitions
                      (gen/clients (drain-generator)))}))

(def cli-opts
  "Additional CLI options for camu tests."
  [[nil "--s3-endpoint URL" "MinIO/S3 endpoint"
    :default "http://minio:9000"]
   [nil "--camu-binary PATH" "Path to camu binary"
    :default "/jepsen/camu/camu"]
   [nil "--http-port PORT" "HTTP port for camu"
    :default 8080
    :parse-fn #(Integer/parseInt %)]])

(defn -main
  "Entry point for the Jepsen CLI."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn  camu-test
                                 :opt-spec cli-opts})
          (cli/serve-cmd))
   args))
