;;
;;
;;  Copyright 2013-2015 Netflix, Inc.
;;
;;     Licensed under the Apache License, Version 2.0 (the "License");
;;     you may not use this file except in compliance with the License.
;;     You may obtain a copy of the License at
;;
;;         http://www.apache.org/licenses/LICENSE-2.0
;;
;;     Unless required by applicable law or agreed to in writing, software
;;     distributed under the License is distributed on an "AS IS" BASIS,
;;     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;     See the License for the specific language governing permissions and
;;     limitations under the License.
;;
;;

(ns pigpen.functional.fold-test
  (:require [clojure.test :refer [is]]
            [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.join :as pig-join]
            [pigpen.fold :as fold]))

(t/deftest test-vec
  "test pouring values into a vector"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/vec) data)]
    (is (= (t/dump harness command)
           [[1 2 3 4]]))))

(t/deftest test-map
  "test map"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/map #(* % %)) data)]
    (is (= (t/dump harness command)
           [[1 4 9 16]]))))

(t/deftest test-mapcat
  "test mapcat"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/mapcat (fn [x] [(inc x) (dec x)])) data)]
    (is (= (t/dump harness command)
           [[2 0 3 1 4 2 5 3]]))))

(t/deftest test-filter
  "test filter"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/filter even?) data)]
    (is (= (t/dump harness command)
           [[2 4]]))))

(t/deftest test-remove
  "test remove"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/remove even?) data)]
    (is (= (t/dump harness command)
           [[1 3]]))))

(t/deftest test-keep
  "test keep"
  [harness]
  (let [data (t/data harness [1 2 nil 3 4])
        command (pig-join/fold (fold/keep identity) data)]
    (is (= (t/dump harness command)
           [[1 2 3 4]]))))

(t/deftest test-distinct
  "test distinct"
  [harness]
  (let [data (t/data harness [1 2 3 4 1 2 3 4])
        command (pig-join/fold (fold/distinct) data)]
    (is (= (t/dump harness command)
           [#{1 2 3 4}]))))

(t/deftest test-take
  "test take"
  [harness]
  (let [raw-data #{1 2 3 4}
        data (t/data harness raw-data)
        command (pig-join/fold (fold/take 2) data)
        [result] (t/dump harness command)]
    (is (= (count result) 2))
    (is (every? raw-data result))))

;; There is no defined order, so any item is the first or last
(t/deftest test-first
  "test first"
  [harness]
  (let [raw-data #{1 2 3 4}
        data (t/data harness raw-data)
        command (pig-join/fold (fold/first) data)
        result (t/dump harness command)]
    (is (= (count result) 1))
    (is (every? raw-data result))))

(t/deftest test-last
  "test last"
  [harness]
  (let [raw-data #{1 2 3 4}
        data (t/data harness raw-data)
        command (pig-join/fold (fold/last) data)
        result (t/dump harness command)]
    (is (= (count result) 1))
    (is (every? raw-data result))))

(t/deftest test-sort
  "test sort"
  [harness]
  (let [data (t/data harness [2 4 1 3 2 3 5])
        command (pig-join/fold (fold/sort) data)]
    (is (= (t/dump harness command)
           [[1 2 2 3 3 4 5]]))))

(t/deftest test-sort-desc
  "test sort descending"
  [harness]
  (let [data (t/data harness [2 4 1 3 2 3 5])
        command (pig-join/fold (fold/sort > (fold/vec)) data)]
    (is (= (t/dump harness command)
           [[5 4 3 3 2 2 1]]))))

(t/deftest test-sort-by
  "test sort by"
  [harness]
  (let [data (t/data harness [{:foo 1 :bar "d"}
                              {:foo 2 :bar "c"}
                              {:foo 3 :bar "b"}
                              {:foo 4 :bar "a"}])
        command (pig-join/fold (fold/sort-by :bar) data)]
    (is (= (t/dump harness command)
           [[{:foo 4, :bar "a"}
             {:foo 3, :bar "b"}
             {:foo 2, :bar "c"}
             {:foo 1, :bar "d"}]]))))

(t/deftest test-sort-by-desc
  "test sort by descending"
  [harness]
  (let [data (t/data harness [{:foo 1 :bar "d"}
                              {:foo 2 :bar "c"}
                              {:foo 3 :bar "b"}
                              {:foo 4 :bar "a"}])
        command (pig-join/fold (fold/sort-by :bar (comp - compare) (fold/vec)) data)]
    (is (= (t/dump harness command)
           [[{:foo 1, :bar "d"}
             {:foo 2, :bar "c"}
             {:foo 3, :bar "b"}
             {:foo 4, :bar "a"}]]))))

(t/deftest test-juxt-stats
  "test juxt with stats"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/juxt (fold/count) (fold/sum) (fold/avg)) data)]
    (is (= (t/dump harness command)
           [[4 10 5/2]]))))

(t/deftest test-juxt-min-max
  "test juxt with min/max"
  [harness]
  (let [data (t/data harness [{:foo 2 :bar "c"}
                              {:foo 1 :bar "d"}
                              {:foo 4 :bar "a"}
                              {:foo 3 :bar "b"}])
        command (pig-join/fold (fold/juxt
                                 (->> (fold/map :foo) (fold/min))
                                 (->> (fold/map :foo) (fold/max))) data)]
    (is (= (t/dump harness command)
           [[1 4]]))))

(t/deftest test-count
  "test count"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/count) data)]
    (is (= (t/dump harness command)
           [4]))))

(t/deftest test-sum
  "test sum"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/sum) data)]
    (is (= (t/dump harness command)
           [10]))))

(t/deftest test-avg
  "test avg"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/avg) data)]
    (is (= (t/dump harness command)
           [5/2]))))

(t/deftest test-avg-with-cogroup
  "test cogroup with two folds"
  [harness]
  (let [foos (t/data harness [1 2 2 3 3 3])
        bars (t/data harness [1 1 1 2 2 3])
        command (pig-join/cogroup [(foos :on identity, :fold (fold/sum))
                                   (bars :on identity, :fold (fold/avg))]
                                  (fn [key f b] [key f b]))]
    (is (= (set (t/dump harness command))
           #{[1 1 1] [2 4 2] [3 9 3]}))))

(t/deftest test-top
  "test top"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/top 2) data)]
    (is (= (t/dump harness command)
           [[1 2]]))))

(t/deftest test-top-desc
  "test top descending"
  [harness]
  (let [data (t/data harness [1 2 3 4])
        command (pig-join/fold (fold/top > 2) data)]
    (is (= (t/dump harness command)
           [[4 3]]))))

(t/deftest test-top-by
  "test top-by"
  [harness]
  (let [data (t/data harness [{:foo 1 :bar "d"}
                              {:foo 2 :bar "c"}
                              {:foo 3 :bar "b"}
                              {:foo 4 :bar "a"}])
        command (pig-join/fold (fold/top-by :bar 2) data)]
    (is (= (t/dump harness command)
           [[{:foo 4, :bar "a"}
             {:foo 3, :bar "b"}]]))))

(t/deftest test-top-by-desc
  "test top-by descending"
  [harness]
  (let [data (t/data harness [{:foo 1 :bar "d"}
                              {:foo 2 :bar "c"}
                              {:foo 3 :bar "b"}
                              {:foo 4 :bar "a"}])
        command (pig-join/fold (fold/top-by :bar (comp - compare) 2) data)]
    (is (= (t/dump harness command)
           [[{:foo 1, :bar "d"}
             {:foo 2, :bar "c"}]]))))

(t/deftest test-min
  "test min"
  [harness]
  (let [data (t/data harness [2 1 4 3])
        command (pig-join/fold (fold/min) data)]
    (is (= (t/dump harness command)
           [1]))))

(t/deftest test-min+map
  "test map then min"
  [harness]
  (let [data (t/data harness [{:foo 2 :bar "c"}
                              {:foo 1 :bar "d"}
                              {:foo 4 :bar "a"}
                              {:foo 3 :bar "b"}])
        command (pig-join/fold (->> (fold/map :foo) (fold/min)) data)]
    (is (= (t/dump harness command)
           [1]))))

(t/deftest test-min-key
  "test min-key"
  [harness]
  (let [data (t/data harness [{:foo 2 :bar "c"}
                              {:foo 1 :bar "d"}
                              {:foo 4 :bar "a"}
                              {:foo 3 :bar "b"}])
        command (pig-join/fold (fold/min-key :foo) data)]
    (is (= (t/dump harness command)
           [{:foo 1 :bar "d"}]))))

(t/deftest test-max
  "test max"
  [harness]
  (let [data (t/data harness [2 1 4 3])
        command (pig-join/fold (fold/max) data)]
    (is (= (t/dump harness command)
           [4]))))

(t/deftest test-max-key
  "test max-key"
  [harness]
  (let [data (t/data harness [{:foo 2 :bar "c"}
                              {:foo 1 :bar "d"}
                              {:foo 4 :bar "a"}
                              {:foo 3 :bar "b"}])
        command (pig-join/fold (fold/max-key :foo) data)]
    (is (= (t/dump harness command)
           [{:foo 4 :bar "a"}]))))
