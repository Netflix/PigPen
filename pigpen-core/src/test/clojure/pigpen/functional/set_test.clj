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

(ns pigpen.functional.set-test
  (:require [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff]]
            [pigpen.set :as pig-set]
            [pigpen.fold :as fold]))

(t/deftest test-distinct
  "normal distinct"
  [harness]
  (test-diff
    (->>
      (t/data harness [5 1 2 3 4 3 2 1 5])
      (pig-set/distinct)
      (t/dump harness)
      (sort))
    '[1 2 3 4 5]))

(t/deftest test-concat
  "normal concat"
  [harness]
  (let [data1 (t/data harness [1 2 3])
        data2 (t/data harness [2 3 4])
        data3 (t/data harness [3 4 5])

        command (pig-set/concat data1 data2 data3)]
    (test-diff
      (sort (t/dump harness command))
      '[1 2 2
        3 3 3
        4 4 5])))

(t/deftest test-union
  "normal union"
  [harness]
  (let [data1 (t/data harness [1 2 3])
        data2 (t/data harness [2 3 4])
        data3 (t/data harness [3 4 5])

        command (pig-set/union data1 data2 data3)]
    (test-diff
      (set (t/dump harness command))
      '#{1 2 3 4 5})))

(t/deftest test-union-multiset
  "normal union multiset"
  [harness]
  (let [data1 (t/data harness [1 2 3])
        data2 (t/data harness [2 3 4])
        data3 (t/data harness [3 4 5])

        command (pig-set/union-multiset data1 data2 data3)]
    (test-diff
      (sort (t/dump harness command))
      '[1 2 2
        3 3 3
        4 4 5])))

(t/deftest test-intersection
  "normal intersection"
  [harness]
  (let [data1 (t/data harness [1 2 3 3])
        data2 (t/data harness [3 2 3 4 3])
        data3 (t/data harness [3 4 3 5 2])

        command (pig-set/intersection data1 data2 data3)]
    (test-diff
      (sort (t/dump harness command))
      '[2 3])))

(t/deftest test-intersection-multiset
  "normal intersection multiset"
  [harness]
  (let [data1 (t/data harness [1 2 3 3])
        data2 (t/data harness [3 2 3 4 3])
        data3 (t/data harness [3 4 3 5 2])

        command (pig-set/intersection-multiset data1 data2 data3)]
    (test-diff
      (sort (t/dump harness command))
      '[2 3 3])))

(t/deftest test-difference
  "normal difference"
  [harness]
  (let [data1 (t/data harness [1 2 3 3 3 4 5])
        data2 (t/data harness [1 2])
        data3 (t/data harness [4 5])

        command (pig-set/difference data1 data2 data3)]
    (test-diff
      (sort (t/dump harness command))
      '[3])))

(t/deftest test-difference-multiset
  "normal difference multiset"
  [harness]
  (let [data1 (t/data harness [1 2 3 3 3 4 5])
        data2 (t/data harness [1 2 3])
        data3 (t/data harness [3 4 5])

        command (pig-set/difference-multiset data1 data2 data3)]
    (test-diff
      (sort (t/dump harness command))
      '[3])))
