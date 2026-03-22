(ns jepsen.camu.db
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]))

(def camu-bin "/opt/camu/camu")
(def camu-pid "/var/run/camu.pid")
(def camu-log "/var/log/camu.log")
(def camu-data "/var/lib/camu")
(def camu-config "/etc/camu/camu.yaml")

(defn write-config!
  "Writes the camu YAML config file matching camu's actual config schema."
  [test node]
  (let [s3-endpoint (:s3-endpoint test "http://minio:9000")
        http-port   (:http-port test 8080)
        config      (str "server:\n"
                         "  address: \":" http-port "\"\n"
                         "  instance_id: \"" node "\"\n"
                         "\n"
                         "storage:\n"
                         "  bucket: \"camu-data\"\n"
                         "  region: \"us-east-1\"\n"
                         "  endpoint: \"" s3-endpoint "\"\n"
                         "  credentials:\n"
                         "    access_key: \"minioadmin\"\n"
                         "    secret_key: \"minioadmin\"\n"
                         "\n"
                         "wal:\n"
                         "  directory: \"" camu-data "/wal\"\n"
                         "  fsync: true\n"
                         "\n"
                         "segments:\n"
                         "  max_size: 1048576\n"
                         "  max_age: \"50ms\"\n"
                         "  compression: \"none\"\n"
                         "\n"
                         "cache:\n"
                         "  directory: \"" camu-data "/cache\"\n"
                         "  max_size: 1073741824\n"
                         "\n"
                         "coordination:\n"
                         "  lease_ttl: \"10s\"\n"
                         "  heartbeat_interval: \"3s\"\n"
                         "  rebalance_delay: \"5s\"\n")]
    (c/exec :mkdir :-p "/etc/camu")
    (c/exec :echo config :> camu-config)))

(defn db
  "Camu database for Jepsen."
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Setting up camu on" node)
      (c/exec :mkdir :-p "/opt/camu"
              (str camu-data "/wal")
              (str camu-data "/cache"))
      ;; Upload the camu binary
      (c/upload (:camu-binary test) camu-bin)
      (c/exec :chmod :+x camu-bin)
      ;; Write config
      (write-config! test node)
      ;; Start camu with 'serve' subcommand
      (cu/start-daemon!
       {:logfile camu-log
        :pidfile camu-pid
        :chdir   camu-data}
       camu-bin
       "serve" "--config" camu-config))

    (teardown! [_ test node]
      (info "Tearing down camu on" node)
      (cu/stop-daemon! camu-pid)
      (c/exec :rm :-rf camu-data camu-log camu-config "/opt/camu"))

    db/LogFiles
    (log-files [_ test node]
      [camu-log])))
