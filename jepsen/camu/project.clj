(defproject jepsen.camu "0.1.0"
  :description "Jepsen tests for Camu commit log"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.12.0"]]
  :main jepsen.camu
  :jvm-opts ["-Xmx4g"])
