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

(ns pigpen.functional.code-test
  (:require [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff]]
            [pigpen.map :as pig-map]
            [pigpen.set :as pig-set]
            [pigpen.fold :as fold]))

(defn test-fn [x]
  (* x x))

(defn test-param [y data]
  (let [z 42]
    (->> data
      (pig-map/map (fn [x] (+ (test-fn x) y z))))))

(t/deftest test-closure
  "make sure fns are available"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2 3])
      (test-param 37)
      (t/dump harness))
    '[80 83 88]))

(t/deftest test-for
  "make sure for doesn't produce hidden vars we can't serialize"
  [harness]
  (test-diff
    (sort
      (t/dump harness
        (apply pig-set/concat
          (for [x [1 2 3]]
            (->>
              (t/data harness [1 2 3])
              (pig-map/map (fn [y] (+ x y))))))))
    [2 3 3 4 4 4 5 5 6]))
