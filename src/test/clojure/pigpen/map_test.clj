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
  (:require [pigpen.util :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.map :as pig]))

(deftest test-map
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [^:local r {:fields '[value]}
          bar 2
          baz (fn [v] v)]
      
      (test-diff
        (pig/map (fn [v] v) r)
        '{:type :bind
          :id bind1
          :description "(fn [v] v)\n"
          :ancestors [{:fields [value]}]
          :func (pigpen.pig/map->bind (clojure.core/let [bar (quote 2)] (fn [v] v)))
          :args [value]
          :requires []
          :fields [value]
          :field-type-in :frozen
          :field-type-out :frozen
          :opts {:type :bind-opts}})
      
      (is (thrown? AssertionError (pig/map nil r)))
      (is (thrown? clojure.lang.Compiler$CompilerException (pig/map foo r)))
      (is (thrown? AssertionError (pig/map bar r)))
      (is (thrown? clojure.lang.Compiler$CompilerException (pig/map baz r)))
      (is (thrown? AssertionError (pig/map (fn [] 42) r)))
      (is (thrown? AssertionError (pig/map #(vector %1 %2) r))))))

(deftest test-mapcat
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [^:local r {:fields '[value]}]
      
      (test-diff
        (pig/mapcat (fn [v] [v]) r)
        '{:type :bind
          :id bind1
          :description "(fn [v] [v])\n"
          :ancestors [{:fields [value]}]
          :func (fn [v] [v])
          :args [value]
          :requires []
          :fields [value]
          :field-type-in :frozen
          :field-type-out :frozen
          :opts {:type :bind-opts}}))))

(deftest test-map-indexed
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [^:local r {:fields '[value]}]
      (test-diff
        (pig/map-indexed vector r)
        '{:type :bind
          :id bind2
          :description nil
          :func (pigpen.pig/map->bind vector)
          :args [$0 value]
          :requires []
          :fields [value]
          :field-type-out :frozen
          :field-type-in :frozen
          :opts {:type :bind-opts}
          :ancestors [{:type :rank
                       :id rank1
                       :description "vector\n"
                       :fields [value $0]
                       :field-type :frozen
                       :opts {:type :rank-opts}
                       :sort-keys []
                       :ancestors [{:fields [value]}]}]}))))

(deftest test-sort
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [^:local r {:fields '[value]}]
      (test-diff
        (pig/sort r)
        '{:type :order
          :id order2
          :description nil
          :fields [value]
          :field-type :frozen
          :opts {:type :order-opts}
          :sort-keys [key :asc]
          :ancestors [{:type :generate
                       :id generate1
                       :description nil
                       :ancestors [{:fields [value]}]
                       :fields [key value]
                       :field-type :frozen
                       :opts {:type :generate-opts}
                       :projections [{:type :projection-flat
                                      :code {:type :code
                                             :expr {:init (clojure.core/require (quote [pigpen.pig]))
                                                    :func (pigpen.pig/exec-multi :frozen :native [(pigpen.pig/map->bind clojure.core/identity)])}
                                             :return "DataBag"
                                             :args [value]}
                                      :alias key}
                                     {:type :projection-field
                                      :field value
                                      :alias value}]}]}))))

(deftest test-sort-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [^:local r {:fields '[value]}]
      (test-diff
        (pig/sort-by :a r)
        '{
          :type :order
          :id order2
          :description ":a\n"
          :fields [value]
          :field-type :frozen
          :opts {:type :order-opts}
          :sort-keys [key :asc]
          :ancestors [{:type :generate
                       :id generate1
                       :description nil
                       :ancestors [{:fields [value]}]
                       :fields [key value]
                       :field-type :frozen
                       :opts {:type :generate-opts}
                       :projections [{:type :projection-flat
                                      :code {:type :code
                                             :expr {:init (clojure.core/require (quote [pigpen.pig]))
                                                    :func (pigpen.pig/exec-multi :frozen :native [(pigpen.pig/map->bind :a)])}
                                             :return "DataBag"
                                             :args [value]}
                                      :alias key}
                                     {:type :projection-field
                                      :field value
                                      :alias value}]}]}))))
