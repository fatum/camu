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

(defn capture-node-logs?
  []
  (not= "1" (System/getenv "CAMU_DISABLE_NODE_LOGS")))

(defn write-config!
  "Writes the camu YAML config file matching camu's actual config schema."
  [test node]
  (let [s3-endpoint (:s3-endpoint test "http://minio:9000")
        http-port   (:http-port test 8080)
        wal-chunk-size (:wal-chunk-size test 67108864)
        segment-max-size (:segment-max-size test 104857600)
        segment-max-age (:segment-max-age test "1m")
        config      (str "server:\n"
                         "  address: \":" http-port "\"\n"
                         "  internal_address: \":8081\"\n"
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
                         "  chunk_size: " wal-chunk-size "\n"
                         "\n"
                         "segments:\n"
                         "  max_size: " segment-max-size "\n"
                         "  max_age: \"" segment-max-age "\"\n"
                         "  compression: \"none\"\n"
                         "\n"
                         "cache:\n"
                         "  directory: \"" camu-data "/cache\"\n"
                         "  max_size: 1073741824\n"
                         "\n"
                         "coordination:\n"
                         "  lease_ttl: \"6s\"\n"
                         "  instance_ttl: \"8s\"\n"
                         "  heartbeat_interval: \"2s\"\n"
                         "  rebalance_delay: \"2s\"\n")]
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
      ;; Start camu as a daemon. In long-run modes we can disable per-node log
      ;; capture to avoid filling disk with verbose server logs.
      (when (capture-node-logs?)
        ;; Also symlink log to container stdout so `docker logs` works.
        (c/exec :ln :-sf "/proc/1/fd/1" "/var/log/camu-docker.log"))
      (cu/start-daemon!
       {:logfile (if (capture-node-logs?) camu-log "/dev/null")
        :pidfile camu-pid
        :chdir   camu-data}
       camu-bin
       "serve" "--config" camu-config)
      ;; Tail log to container stdout in background for docker logs visibility.
      (when (capture-node-logs?)
        (c/exec :bash :-c (str "tail -f " camu-log " > /proc/1/fd/1 2>/dev/null &"))))

    (teardown! [_ test node]
      (info "Tearing down camu on" node)
      (cu/stop-daemon! camu-pid)
      (c/exec
       :bash :-lc
       (str "rm -rf " camu-log " " camu-config " /opt/camu; "
            "if [ -d " camu-data " ]; then "
            "find " camu-data " -depth -exec rm -rf {} +; "
            "fi")))

    db/LogFiles
    (log-files [_ test node]
      (if (capture-node-logs?) [camu-log] []))))
