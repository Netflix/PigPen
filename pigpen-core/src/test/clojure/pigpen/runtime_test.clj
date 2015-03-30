;;
;;
;;  Copyright 2015 Netflix, Inc.
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

(ns pigpen.runtime-test
  (:require [clojure.test :refer :all]
            [pigpen.runtime :refer :all]
            [clojure.edn]))

(defn xf->f [xf]
  (partial (xf conj) []))

(deftest test-map->bind
  (let [f (xf->f (map->bind +))]
    (is (= (f [1 2 3]) [[6]]))
    (is (= (f [2 4 6]) [[12]]))))

(deftest test-mapcat->bind
  (let [f (xf->f (mapcat->bind identity))]
    (is (= (f [[1 2 3]]) [[1] [2] [3]]))
    (is (= (f [[2 4 6]]) [[2] [4] [6]]))))

(deftest test-filter->bind
  (let [f (xf->f (filter->bind even?))]
    (is (= (f [1]) []))
    (is (= (f [2]) [[2]]))
    (is (= (f [3]) []))
    (is (= (f [4]) [[4]]))))

(deftest test-key-selector->bind
  (let [f (xf->f (key-selector->bind first))]
    (is (= (f [[1 2 3]]) [[1 [1 2 3]]]))
    (is (= (f [[2 4 6]]) [[2 [2 4 6]]])))
  (let [f (xf->f (key-selector->bind :foo))]
    (is (= (f [{:foo 1, :bar 2}]) [[1 {:foo 1, :bar 2}]]))))

(deftest test-keyword-field-selector->bind
  (let [f (xf->f (keyword-field-selector->bind [:foo :bar :baz]))]
    (is (= (f [{:foo 1, :bar 2, :baz 3}]) [[1 2 3]]))))

(deftest test-indexed-field-selector->bind
  (let [f (xf->f (indexed-field-selector->bind 2 clojure.string/join))]
    (is (= (f [[1 2 3 4]]) [[1 2 "34"]]))))

(deftest test-args->map
  (let [f (args->map #(* 2 %))]
    (is (= (f "a" 2 "b" 3)
           {:a 4 :b 6}))))

(deftest test-debug
  (is (= "class java.lang.Long\t2\tclass java.lang.String\tfoo\tclass java.lang.String\tbar"
         (debug 2 "foo" "bar"))))

(deftest test-eval-string
  (let [f-str "(fn [x] (* x x))"
        f (eval-string f-str)]
    (is (= (f 2) 4))
    (is (= (f 42) 1764))))

(deftest test-exec

  (let [command (comp
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/pre-process :none :native))
                  (pigpen.runtime/map->bind vector)
                  (pigpen.runtime/map->bind identity)
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/post-process :none :native)))]

    (is (= ((xf->f command) [1 2])
           [[[1 2]]])))

  (let [command (comp
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/pre-process :none :native))
                  (pigpen.runtime/map->bind clojure.edn/read-string)
                  (pigpen.runtime/map->bind identity)
                  (pigpen.runtime/filter->bind (constantly true))
                  (pigpen.runtime/mapcat->bind vector)
                  (pigpen.runtime/map->bind clojure.core/pr-str)
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/post-process :none :native)))]

    (is (= ((xf->f command) ["1"])
           [["1"]])))

  (let [command (comp
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/pre-process :none :native))
                  (pigpen.runtime/map->bind clojure.edn/read-string)
                  (pigpen.runtime/mapcat->bind (fn [x] [x (+ x 1) (+ x 2)]))
                  (pigpen.runtime/map->bind clojure.core/pr-str)
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/post-process :none :native)))]

    (is (= ((xf->f command) ["1"])
           [["1"] ["2"] ["3"]])))

  (let [command (comp
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/pre-process :none :native))
                  (pigpen.runtime/map->bind clojure.edn/read-string)
                  (pigpen.runtime/mapcat->bind (fn [x] [x (* x 2)]))
                  (pigpen.runtime/mapcat->bind (fn [x] [x (* x 2)]))
                  (pigpen.runtime/mapcat->bind (fn [x] [x (* x 2)]))
                  (pigpen.runtime/map->bind clojure.core/pr-str)
                  (pigpen.runtime/process->bind
                    (pigpen.runtime/post-process :none :native)))]

    (is (= ((xf->f command) ["1"])
           [["1"] ["2"] ["2"] ["4"] ["2"] ["4"] ["4"] ["8"]]))))
