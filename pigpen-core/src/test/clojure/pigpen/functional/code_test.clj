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

(ns pigpen.functional.code-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff]]
            [pigpen.core :as pig]
            [pigpen.fold :as fold]))

(defn test-fn [x]
  (* x x))

(defn test-param [y data]
  (let [z 42]
    (->> data
      (pig/map (fn [x] (+ (test-fn x) y z))))))

(deftest test-closure
  (let [data (pig/return [1 2 3])
        command (test-param 37 data)]
    (test-diff
      (pig/dump command)
      '[80 83 88])))

(deftest test-for
  (is (= (pig/dump
           (apply pig/concat
             (for [x [1 2 3]]
               (->>
                 (pig/return [1 2 3])
                 (pig/map (fn [y] (+ x y)))))))
         [4 3 2 5 4 3 6 5 4])))
