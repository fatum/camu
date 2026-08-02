(ns jepsen.camu.client-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.client :as jepsen-client]
            [jepsen.camu.client :as camu-client]))

(deftest sql-drain-paginates-beyond-the-api-default-limit
  (let [calls (atom [])
        page-size camu-client/sql-drain-page-size
        first-page (vec (repeat page-size ["first-page"]))
        second-page [["last-row"]]]
    (with-redefs [camu-client/sql-query-with-candidates!
                  (fn [_nodes _topic sql limit _deadline]
                    (swap! calls conj {:sql sql :limit limit})
                    {:columns [{:name "key" :type "BLOB"}]
                     :rows (if (zero? (count @calls)) first-page
                               (if (= 1 (count @calls)) first-page second-page))})]
      (let [result (camu-client/sql-drain-with-candidates! ["n1"] "events" 1000 false)]
        (is (= (inc page-size) (count (:rows result))))
        (is (= [0 page-size]
               (mapv #(Long/parseLong (second (re-find #"OFFSET ([0-9]+)" (:sql %)))) @calls)))
        (is (= [page-size page-size] (mapv :limit @calls)))))))

(deftest client-dispatch-keeps-sql-and-retention-results-distinct
  (let [test {:topic "events" :nodes ["n1"] :num-partitions 1}
        c (assoc (camu-client/client) :node "n1")]
    (with-redefs [camu-client/sql-query-with-candidates!
                  (fn [& _] {:columns [{:name "n" :type "BIGINT"}] :rows [[7]]})
                  camu-client/native-retention-state!
                  (fn [_topic _partitions]
                    [{:partition 0 :source-segment-count 0 :exported-through-offset 7}])]
      (let [sql-op (jepsen-client/invoke! c test
                                            {:type :invoke :f :sql-query
                                             :value {:sql "select 7" :final false}})
            retention-op (jepsen-client/invoke! c test
                                                  {:type :invoke :f :retention-state :value {}})]
        (is (= :ok (:type sql-op)))
        (is (= [[7]] (get-in sql-op [:value :rows])))
        (is (nil? (get-in sql-op [:value :partitions])))
        (is (= :ok (:type retention-op)))
        (is (= [{:partition 0 :source-segment-count 0 :exported-through-offset 7}]
               (get-in retention-op [:value :partitions])))
        (is (nil? (get-in retention-op [:value :rows])))))))
