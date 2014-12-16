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

(ns pigpen.map-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.map :as pig]))

(deftest test-map
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [^:local r1 {:fields '[r0/value]}
          bar 2
          baz (fn [v] v)]

      (test-diff
        (pig/map (fn [v] v) r1)
        '{:type :bind
          :id bind1
          :description "(fn [v] v)\n"
          :ancestors [{:fields [r0/value]}]
          :func (pigpen.runtime/map->bind
                  (pigpen.runtime/with-ns pigpen.map-test
                    (clojure.core/let [bar (quote 2)]
                      (fn [v] v))))
          :args [r0/value]
          :requires []
          :fields [bind1/value]
          :field-type-in :frozen
          :field-type-out :frozen
          :opts {:type :bind-opts}})

      (is (thrown? AssertionError (pig/map nil r1)))
      (is (thrown? clojure.lang.Compiler$CompilerException (pig/map foo r1)))
      (is (thrown? AssertionError (pig/map bar r1)))
      (is (thrown? clojure.lang.Compiler$CompilerException (pig/map baz r1)))
      (is (thrown? AssertionError (pig/map (fn [] 42) r1)))
      (is (thrown? AssertionError (pig/map #(vector %1 %2) r1))))))

(deftest test-mapcat
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [^:local r1 {:fields '[r0/value]}]

      (test-diff
        (pig/mapcat (fn [v] [v]) r1)
        '{:type :bind
          :id bind1
          :description "(fn [v] [v])\n"
          :ancestors [{:fields [r0/value]}]
          :func (pigpen.runtime/mapcat->bind
                  (pigpen.runtime/with-ns pigpen.map-test
                    (fn [v] [v])))
          :args [r0/value]
          :requires []
          :fields [bind1/value]
          :field-type-in :frozen
          :field-type-out :frozen
          :opts {:type :bind-opts}}))))

(deftest test-map-indexed
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [^:local r0 {:fields '[r0/value]}]
      (test-diff
        (pig/map-indexed vector r0)
        '{:type :bind
          :id bind2
          :description nil
          :func (pigpen.runtime/map->bind
                  (pigpen.runtime/with-ns pigpen.map-test
                    vector))
          :args [rank1/index rank1/value]
          :requires []
          :fields [bind2/value]
          :field-type-out :frozen
          :field-type-in :frozen
          :opts {:type :bind-opts}
          :ancestors [{:type :rank
                       :id rank1
                       :description "vector\n"
                       :fields [rank1/index rank1/value]
                       :field-type :frozen
                       :opts {:type :rank-opts}
                       :ancestors [{:fields [r0/value]}]}]}))))

(deftest test-sort
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [^:local r0 {:fields '[r0/value]}]
      (test-diff
        (pig/sort r0)
        '{:type :order
          :id order2
          :description nil
          :fields [order2/value]
          :field-type :frozen
          :opts {:type :order-opts}
          :key bind1/key
          :comp :asc
          :ancestors [{:type :bind
                       :id bind1
                       :description nil
                       :func (pigpen.runtime/key-selector->bind clojure.core/identity)
                       :args [r0/value]
                       :requires []
                       :fields [bind1/key bind1/value]
                       :field-type-in :frozen
                       :field-type-out :native-key-frozen-val
                       :ancestors [{:fields [r0/value]}]
                       :opts {:type :bind-opts
                              :implicit-schema true}}]}))))

(deftest test-sort-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [^:local r0 {:fields '[r0/value]}]
      (test-diff
        (pig/sort-by :a r0)
        '{:type :order
          :id order2
          :description ":a\n"
          :fields [order2/value]
          :field-type :frozen
          :opts {:type :order-opts}
          :key bind1/key
          :comp :asc
          :ancestors [{:type :bind
                       :id bind1
                       :description nil
                       :func (pigpen.runtime/key-selector->bind
                               (pigpen.runtime/with-ns pigpen.map-test :a))
                       :args [r0/value]
                       :requires []
                       :fields [bind1/key bind1/value]
                       :field-type-in :frozen
                       :field-type-out :native-key-frozen-val
                       :ancestors [{:fields [r0/value]}]
                       :opts {:type :bind-opts
                              :implicit-schema true}}]}))))
