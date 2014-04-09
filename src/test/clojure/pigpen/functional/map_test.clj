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

(ns pigpen.functional.map-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff]]
            [pigpen.core :as pig]
            [pigpen.fold :as fold]))

(deftest test-map
  
  (let [data (pig/return [{:x 1, :y 2}
                         {:x 2, :y 4}])
        command (pig/map (fn [{:keys [x y]}] (+ x y)) data)]
    (test-diff
      (pig/dump command)
      '[3 6])))
  
(deftest test-mapcat
  
  (let [data (pig/return [{:x 1, :y 2}
                         {:x 2, :y 4}])
        command (pig/mapcat (juxt :x :y) data)]
    (test-diff
      (pig/dump command)
      '[1 2 2 4])))

(deftest test-map-indexed
  
  (let [data (pig/return [{:a 2} {:a 1} {:a 3}])
        command (pig/map-indexed vector data)]
    (test-diff
      (pig/dump command)
      '[[0 {:a 2}] [1 {:a 1}] [2 {:a 3}]]))
  
  (let [command (->>
                  (pig/return [{:a 2} {:a 1} {:a 3}])
                  (pig/sort-by :a)
                  (pig/map-indexed vector))]
    (test-diff
      (pig/dump command)
      '[[0 {:a 1}] [1 {:a 2}] [2 {:a 3}]])))

(deftest test-sort
  
  (let [data (pig/return [2 1 4 3])
        command (pig/sort data)]
    (test-diff
      (pig/dump command)
      '[1 2 3 4]))
  
  (let [data (pig/return [2 1 4 3])
        command (pig/sort :desc data)]
    (test-diff
      (pig/dump command)
      '[4 3 2 1])))

(deftest test-sort-by
  
  (let [data (pig/return [{:a 2} {:a 1} {:a 3}])
        command (pig/sort-by :a data)]
    (test-diff
      (pig/dump command)
      '[{:a 1} {:a 2} {:a 3}]))
  
  (let [data (pig/return [{:a 2} {:a 1} {:a 3}])
       command (pig/sort-by :a :desc data)]
   (test-diff
     (pig/dump command)
     '[{:a 3} {:a 2} {:a 1}]))
  
  (let [data (pig/return [1 2 3 1 2 3 1 2 3])
        command (pig/sort-by identity data)]
    (is (= (pig/dump command)
           [1 1 1 2 2 2 3 3 3]))))

(deftest test-map+fold
  (is (= (pig/dump
           (->> (pig/return [-2 -1 0 1 2])
             (pig/map #(> % 0))
             (pig/fold (->> (fold/filter identity) (fold/count)))))
         [2])))

(deftest test-map+reduce
  (is (= (pig/dump
           (->> (pig/return [-2 -1 0 1 2])
             (pig/map inc)
             (pig/fold (->> (fold/filter #(> % 0)) (fold/count)))))
         [3])))
