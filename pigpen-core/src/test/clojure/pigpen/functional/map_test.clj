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

(ns pigpen.functional.map-test
  (:require [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff]]
            [pigpen.join :as pig-join]
            [pigpen.map :as pig-map]
            [pigpen.fold :as fold]))

(t/deftest test-map
  "normal map"
  [harness]
  (test-diff
    (->>
      (t/data harness [{:x 1, :y 2}
                       {:x 2, :y 4}])
      (pig-map/map (fn [{:keys [x y]}] (+ x y)))
      (t/dump harness))
    '[3 6]))

(t/deftest test-mapcat
  "normal mapcat"
  [harness]
  (test-diff
    (->>
      (t/data harness [{:x 1, :y 2}
                       {:x 2, :y 4}])
      (pig-map/mapcat (juxt :x :y))
      (t/dump harness))
    '[1 2 2 4]))

(t/deftest test-map-indexed
  "normal map-indexed"
  [harness]
  (test-diff
    (->>
      (t/data harness [{:a 2} {:a 1} {:a 3}])
      (pig-map/map-indexed vector)
      (t/dump harness))
    '[[0 {:a 2}] [1 {:a 1}] [2 {:a 3}]]))

(t/deftest test-map-indexed+sort
  "sort + map-indexed test"
  [harness]
  (test-diff
    (->>
      (t/data harness [{:a 2} {:a 1} {:a 3}])
      (pig-map/sort-by :a)
      (pig-map/map-indexed vector)
      (t/dump harness))
    '[[0 {:a 1}] [1 {:a 2}] [2 {:a 3}]]))

(t/deftest test-sort
  "normal sort"
  [harness]
  (test-diff
    (->>
      (t/data harness [2 1 4 3])
      (pig-map/sort)
      (t/dump harness))
    '[1 2 3 4]))

(t/deftest test-sort-desc
  "descending sort"
  [harness]
  (test-diff
    (->>
      (t/data harness [2 1 4 3])
      (pig-map/sort :desc)
      (t/dump harness))
    '[4 3 2 1]))

(t/deftest test-sort-by
  "normal sort-by"
  [harness]
  (test-diff
    (->>
      (t/data harness [{:a 2} {:a 1} {:a 3}])
      (pig-map/sort-by :a)
      (t/dump harness))
    '[{:a 1} {:a 2} {:a 3}]))

(t/deftest test-sort-by-desc
  "descending sort-by"
  [harness]
  (test-diff
    (->>
      (t/data harness [{:a 2} {:a 1} {:a 3}])
      (pig-map/sort-by :a :desc)
      (t/dump harness))
    '[{:a 3} {:a 2} {:a 1}]))

(t/deftest test-sort-by-with-duplicates
  "sort-by with duplicate input values"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2 3 1 2 3 1 2 3])
      (pig-map/sort-by identity)
      (t/dump harness))
    [1 1 1 2 2 2 3 3 3]))

(t/deftest test-map+fold1
  "map followed by a fold with a filter"
  [harness]
  (test-diff
    (->>
      (t/data harness [-2 -1 0 1 2])
      (pig-map/map inc)
      (pig-join/fold (->> (fold/filter #(> % 0)) (fold/count)))
      (t/dump harness))
    [3]))

(t/deftest test-map+fold2
  "map to boolean followed by a fold with a filter"
  [harness]
  (test-diff
    (->>
      (t/data harness [-2 -1 0 1 2])
      (pig-map/map #(> % 0))
      (pig-join/fold (->> (fold/filter identity) (fold/count)))
      (t/dump harness))
    [2]))

(t/deftest test-map-nil
  "nils should stay nil"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2])
      (pig-map/map (constantly nil))
      (t/dump harness))
    [nil nil]))

(t/deftest test-mapcat-nil
  "nils should stay nil"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2])
      (pig-map/mapcat #(repeat % nil))
      (t/dump harness))
    [nil nil nil]))
