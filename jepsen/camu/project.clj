(defproject jepsen.camu "0.1.0"
  :description "Jepsen tests for Camu commit log"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [clj-http "3.12.3"]
                 [cheshire "5.12.0"]
                 [org.slf4j/slf4j-api "2.0.3"]
                 [org.apache.kafka/kafka-clients "3.8.0" :exclusions [org.slf4j/slf4j-api]]]
  :main jepsen.camu
  :jvm-opts ["-Xmx4g"])
