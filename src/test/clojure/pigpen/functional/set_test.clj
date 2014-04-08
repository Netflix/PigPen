;;
;;
;;  Copyright 2013 Netflix, Inc.
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
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff]]
            [pigpen.core :as pig]
            [pigpen.fold :as fold]))

(deftest test-union
  (let [data1 (pig/return [1 2 3])
        data2 (pig/return [2 3 4])
        data3 (pig/return [3 4 5])
        
        command (pig/union data1 data2 data3)]
    (test-diff
      (set (pig/dump command))
      '#{1 2 3 4 5})))

(deftest test-union-multiset
  (let [data1 (pig/return [1 2 3])
        data2 (pig/return [2 3 4])
        data3 (pig/return [3 4 5])
        
        command (pig/union-multiset data1 data2 data3)]
    (test-diff
      (sort (pig/dump command))
      '[1 2 2
        3 3 3
        4 4 5])))

(deftest test-intersection
  (let [data1 (pig/return [1 2 3 3])
        data2 (pig/return [3 2 3 4 3])
        data3 (pig/return [3 4 3 5 2])
        
        command (pig/intersection data1 data2 data3)]
    (test-diff
      (sort (pig/dump command))
      '[2 3])))

(deftest test-intersection-multiset
  (let [data1 (pig/return [1 2 3 3])
        data2 (pig/return [3 2 3 4 3])
        data3 (pig/return [3 4 3 5 2])
        
        command (pig/intersection-multiset data1 data2 data3)]
    (test-diff
      (sort (pig/dump command))
      '[2 3 3])))

(deftest test-difference
  (let [data1 (pig/return [1 2 3 3 3 4 5])
        data2 (pig/return [1 2])
        data3 (pig/return [4 5])
        
        command (pig/difference data1 data2 data3)]
    (test-diff
      (sort (pig/dump command))
      '[3])))

(deftest test-difference-multiset
  (let [data1 (pig/return [1 2 3 3 3 4 5])
        data2 (pig/return [1 2 3])
        data3 (pig/return [3 4 5])
        
        command (pig/difference-multiset data1 data2 data3)]
    (test-diff
      (sort (pig/dump command))
      '[3])))
