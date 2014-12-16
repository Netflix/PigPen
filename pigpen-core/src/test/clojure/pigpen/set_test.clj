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

(ns pigpen.set-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.raw :as raw]
            [pigpen.set :as pig]))

(deftest test-split-opts-relations
  (is (=
        (#'pigpen.set/split-opts-relations [{:o1 1 :o2 2} {:id 1, :type :load} {:id 2, :type :load} {:id 3, :type :load}])
        [{:o1 1 :o2 2} [{:id 1, :type :load} {:id 2, :type :load} {:id 3, :type :load}]])))

(deftest test-pig-intersection
  (is (= #{1}
         (pig/pig-intersection [1 1 1] [1 1] [1 1 1])))
  (is (= #{1}
         (pig/pig-intersection [1 1 1] [1 1] [1 1 1])))
  (is (= #{1}
         (pig/pig-intersection [1 1 1]))))

(deftest test-pig-intersection-multiset
  (is (= [1 1]
         (pig/pig-intersection-multiset [1 1 1] [1 1])))
  (is (= []
         (pig/pig-intersection-multiset [1 1 1] [] [1 1])))
  (is (= [1 1 1]
         (pig/pig-intersection-multiset [1 1 1]))))

(deftest test-pig-difference
  (is (= #{}
         (pig/pig-difference [1 1 1] [1 1] [1 1 1])))
  (is (= #{1}
         (pig/pig-difference [1 1 1] [] [])))
  (is (= #{1}
         (pig/pig-difference [1 1 1]))))

(deftest test-pig-difference-multiset
  (is (= [1]
         (pig/pig-difference-multiset [1 1 1 1] [1 1] [1])))
  (is (= [1 1]
         (pig/pig-difference-multiset [1 1 1] [] [1])))
  (is (= [1 1 1]
         (pig/pig-difference-multiset [1 1 1]))))

(deftest test-distinct
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (let [r0 {:fields '[r0/value]}]

      (test-diff
        (pig/distinct {:parallel 20} r0)
        '{:type :distinct
          :id distinct0
          :description nil
          :ancestors [{:fields [r0/value]}]
          :fields [distinct0/value]
          :field-type :frozen
          :opts {:type :distinct-opts
                 :parallel 20}}))))

(deftest test-union
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (let [r0 '{:id r0, :type :load, :fields [r0/value]}
          r1 '{:id r1, :type :load, :fields [r1/value]}]

      (test-diff
        (pig/union r0 r1)
        '{:type :distinct
          :id distinct0
          :description nil
          :ancestors [{:type :union
                       :id union0
                       :description nil
                       :fields [union0/value]
                       :field-type :frozen
                       :ancestors [{:id r0, :type :load, :fields [r0/value]}
                                   {:id r1, :type :load, :fields [r1/value]}]
                       :opts {:type :union-opts}}]
          :fields [distinct0/value]
          :field-type :frozen
          :opts {:type :distinct-opts}})

      (test-diff
        (pig/union {:parallel 20} r0 r1)
        '{:type :distinct
          :id distinct0
          :description nil
          :ancestors [{:type :union
                       :id union0
                       :description nil
                       :fields [union0/value]
                       :field-type :frozen
                       :ancestors [{:id r0, :type :load, :fields [r0/value]}
                                   {:id r1, :type :load, :fields [r1/value]}]
                       :opts {:type :union-opts}}]
          :fields [distinct0/value]
          :field-type :frozen
          :opts {:type :distinct-opts
                 :parallel 20}}))))

(deftest test-union-multiset
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (let [r0 '{:id r0, :type :load, :fields [r0/value]}
          r1 '{:id r1, :type :load, :fields [r1/value]}]

      (test-diff
        (pig/union-multiset r0 r1)
        '{:type :union
          :id union0
          :description nil
          :fields [union0/value]
          :field-type :frozen
          :ancestors [{:id r0, :type :load, :fields [r0/value]}
                      {:id r1, :type :load, :fields [r1/value]}]
          :opts {:type :union-opts}}))))

(deftest test-intersection
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [r0 '{:id r0, :type :load, :fields [r0/value]}
          r1 '{:id r1, :type :load, :fields [r1/value]}]

      (test-diff
        (pig/intersection {:parallel 20} r0 r1)
        '{:type :bind
          :id bind2
          :description nil
          :func (pigpen.runtime/mapcat->bind pigpen.set/pig-intersection)
          :args [r0/value r1/value]
          :requires [pigpen.set]
          :fields [bind2/value]
          :field-type-in :frozen
          :field-type-out :frozen
          :opts {:type :bind-opts}
          :ancestors [{:type :group
                       :id group1
                       :description nil
                       :keys [r0/value r1/value]
                       :join-types [:optional :optional]
                       :field-dispatch :set
                       :fields [group1/group r0/value r1/value]
                       :field-type :frozen
                       :ancestors [{:id r0, :type :load, :fields [r0/value]}
                                   {:id r1, :type :load, :fields [r1/value]}]
                       :opts {:type :group-opts
                              :parallel 20}}]}))))
