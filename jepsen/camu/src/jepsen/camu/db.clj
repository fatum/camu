(ns jepsen.camu.db
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [clojure.string :as str]))

(def camu-bin "/opt/camu/camu")
(def camu-pid "/var/run/camu.pid")
(def camu-log "/var/log/camu.log")
(def camu-data "/var/lib/camu")
(def camu-config "/etc/camu/camu.yaml")

(defn write-config!
  "Writes the camu YAML config file for this node."
  [test node]
  (let [s3-endpoint (:s3-endpoint test "http://minio:9000")
        http-port   (:http-port test 8080)
        config      (str "server:\n"
                         "  http_port: " http-port "\n"
                         "  node_id: \"" node "\"\n"
                         "\n"
                         "storage:\n"
                         "  backend: s3\n"
                         "  s3:\n"
                         "    endpoint: \"" s3-endpoint "\"\n"
                         "    bucket: \"camu-jepsen\"\n"
                         "    access_key: \"minioadmin\"\n"
                         "    secret_key: \"minioadmin\"\n"
                         "    region: \"us-east-1\"\n"
                         "    force_path_style: true\n"
                         "\n"
                         "log:\n"
                         "  level: info\n"
                         "  file: \"" camu-log "\"\n")]
    (c/exec :mkdir :-p "/etc/camu")
    (c/exec :echo config :> camu-config)))

(defn db
  "Camu database for Jepsen. Takes a test map with :camu-binary pointing
   to the local path of the camu binary to upload."
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Setting up camu on" node)
      (c/exec :mkdir :-p "/opt/camu" camu-data)
      ;; Upload the camu binary
      (c/upload (:camu-binary test) camu-bin)
      (c/exec :chmod :+x camu-bin)
      ;; Write config
      (write-config! test node)
      ;; Start camu as a daemon
      (cu/start-daemon!
       {:logfile camu-log
        :pidfile camu-pid
        :chdir   camu-data}
       camu-bin
       "--config" camu-config))

    (teardown! [_ test node]
      (info "Tearing down camu on" node)
      (cu/stop-daemon! camu-pid)
      (c/exec :rm :-rf camu-data camu-log camu-config)
      (c/exec :rm :-rf "/opt/camu"))

    db/LogFiles
    (log-files [_ test node]
      [camu-log])))
