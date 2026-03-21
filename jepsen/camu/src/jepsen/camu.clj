(ns jepsen.camu
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.camu.client :as client]
            [jepsen.camu.nemesis :as nem]
            [jepsen.camu.checker :as camu-checker]
            [jepsen.camu.db :as db]
            [jepsen [checker :as checker]]))

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
  (let [faults (:faults opts #{:kill :partition :pause})]
    (merge tests/noop-test
           opts
           {:name      "camu"
            :os        jepsen.os/noop
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
                         :timeline            (checker/timeline)
                         :stats               (checker/stats)
                         :perf                (checker/perf)})
            :generator (gen/phases
                        ;; Phase 1: produce under faults
                        (->> (produce-generator)
                             (gen/stagger 1/10)
                             (gen/nemesis (nem/nemesis-generator
                                          (:time-limit opts 120)
                                          faults))
                             (gen/time-limit (:time-limit opts 120)))
                        ;; Phase 2: recovery (nemesis already stopped by
                        ;; nemesis-generator, just wait)
                        (gen/sleep 15)
                        ;; Phase 3: drain all partitions
                        (gen/clients (drain-generator)))})))

(def cli-opts
  "Additional CLI options for camu tests."
  [[nil "--s3-endpoint URL" "MinIO/S3 endpoint"
    :default "http://minio:9000"]
   [nil "--camu-binary PATH" "Path to camu binary"
    :default "/jepsen/camu/camu"]
   [nil "--http-port PORT" "HTTP port for camu"
    :default 8080
    :parse-fn #(Integer/parseInt %)]])

(defn kill-test
  "Test with process kills only."
  [opts]
  (camu-test (assoc opts :faults #{:kill})))

(defn partition-test
  "Test with network partitions only."
  [opts]
  (camu-test (assoc opts :faults #{:partition})))

(defn pause-test
  "Test with process pauses (SIGSTOP/SIGCONT)."
  [opts]
  (camu-test (assoc opts :faults #{:pause})))

(defn combined-test
  "Test with all fault types simultaneously."
  [opts]
  (camu-test (assoc opts :faults #{:kill :partition :pause})))

(defn -main
  "Entry point for the Jepsen CLI."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn  kill-test
                                 :opt-spec cli-opts
                                 :test-name "kill"})
          (cli/single-test-cmd {:test-fn  partition-test
                                 :opt-spec cli-opts
                                 :test-name "partition"})
          (cli/single-test-cmd {:test-fn  pause-test
                                 :opt-spec cli-opts
                                 :test-name "pause"})
          (cli/single-test-cmd {:test-fn  combined-test
                                 :opt-spec cli-opts
                                 :test-name "combined"})
          (cli/serve-cmd))
   args))
