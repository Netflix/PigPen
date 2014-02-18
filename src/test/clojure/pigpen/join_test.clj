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

(ns pigpen.join-test
  (:use clojure.test)
  (:require [pigpen.util :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.join :as pig]))

(deftest test-quote-select-clause
  (test-diff
    (#'pigpen.join/quote-select-clause #{:on :by :key-selector}
                                       '(:from r0 :on (fn [x] x) :type :required))
    {:from 'r0
     :key-selector `(pigpen.code/trap (quote ~(ns-name *ns*)) (~'fn [~'x] ~'x))
     :type :required}))

(deftest test-select->generate
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (test-diff
      (#'pigpen.join/select->generate true '[pigpen.join-test]
                                      '{:from {:fields [value]} :key-selector (fn [x] x)})
      '{:type :generate
        :id generate0
        :description nil
        :projections [{:type :projection-field
                       :field 0
                       :alias key}
                      {:type :projection-field
                       :field 1
                       :alias value}]
        :fields [key value]
        :field-type :frozen
        :opts {:type :generate-opts}
        :ancestors [{:type :bind
                     :id bind0
                     :description nil
                     :func (pigpen.pig/key-selector->bind (fn [x] x))
                     :args [value]
                     :requires [pigpen.join-test]
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :frozen
                     :ancestors [{:fields [value]}]
                     :opts {:type :bind-opts
                            :implicit-schema true}}]})
    
    (test-diff
      (#'pigpen.join/select->generate false '[pigpen.join-test]
                                      '{:from {:fields [value]} :key-selector (fn [x] x)})
      '{:type :generate
        :id generate0
        :description nil
        :projections [{:type :projection-field
                       :field 0
                       :alias key}
                      {:type :projection-field
                       :field 1
                       :alias value}]
        :fields [key value]
        :field-type :frozen
        :opts {:type :generate-opts}
        :ancestors [{:type :bind
                     :id bind0
                     :description nil
                     :func (pigpen.pig/key-selector->bind (fn [x] x))
                     :args [value]
                     :requires [pigpen.join-test]
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :frozen-with-nils
                     :ancestors [{:fields [value]}]
                     :opts {:type :bind-opts
                            :implicit-schema true}}]})))

(deftest test-group-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/group-by (fn [v] (:foo v)) {:parallel 10
                                       :fold (pig/fold-fn +)} {:fields '[value]})
      '{:type :bind
        :id bind8
        :description nil
        :func (pigpen.pig/map->bind (clojure.core/fn [k v] (clojure.lang.MapEntry. k v)))
        :args [field3 field4]
        :requires [pigpen.join-test]
        :fields [value]
        :field-type-out :frozen
        :field-type-in :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :generate
                     :id generate7
                     :description nil
                     :projections [{:type :projection-field, :field group, :alias field3}
                                   {:type :projection-func
                                    :code {:type :code
                                           :expr {:type :fold
                                                  :init (clojure.core/require (quote [pigpen.pig]) (quote [pigpen.join-test]))
                                                  :combinef (pigpen.pig/exec-combinef
                                                              (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                                (clojure.core/eval (quote +))))
                                                  :reducef (pigpen.pig/exec-reducef 0
                                                             (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                               (clojure.core/eval (quote +))))
                                                  :finalf (pigpen.pig/exec-finalf
                                                            (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                              (clojure.core/eval (quote +)))
                                                            (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                              (clojure.core/eval (quote clojure.core/identity))))}
                                           :return "Algebraic"
                                           :args [[[generate2] value]]}
                                    :alias field4}]
                     :fields [field3 field4]
                     :field-type :frozen
                     :opts {:type :generate-opts}
                     :ancestors [{:type :group
                                  :id group6
                                  :description "(fn [v] (:foo v))\n"
                                  :fields [group [[generate2] key] [[generate2] value]]
                                  :field-type :frozen
                                  :join-types [:optional]
                                  :keys [[key]]
                                  :opts {:type :group-opts
                                         :parallel 10}
                                  :ancestors [{:type :generate
                                               :id generate2
                                               :description nil
                                               :field-type :frozen
                                               :fields [key value]
                                               :opts {:type :generate-opts}
                                               :projections [{:type :projection-field, :field 0, :alias key}
                                                             {:type :projection-field, :field 1, :alias value}]
                                               :ancestors [{:type :bind
                                                            :id bind1
                                                            :description nil
                                                            :func (pigpen.pig/key-selector->bind
                                                                    (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                                      (clojure.core/eval
                                                                        (quote (fn [v] (:foo v))))))
                                                            :args [value]
                                                            :requires [pigpen.join-test]
                                                            :fields [value]
                                                            :field-type-in :frozen
                                                            :field-type-out :frozen-with-nils
                                                            :ancestors [{:fields [value]}]
                                                            :opts {:type :bind-opts
                                                                   :implicit-schema true}}]}]}]}]})))

(deftest test-into
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/into [] {:id 'r0 :fields '[value]})
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.pig/map->bind (clojure.core/partial clojure.core/into []))
        :args [[[r0] value]]
        :requires []
        :fields [value]        
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :group
                     :id group1
                     :description "into []"
                     :keys [:pigpen.raw/group-all]
                     :join-types [:optional]
                     :fields [group [[r0] value]]
                     :field-type :frozen
                     :ancestors [{:fields [value], :id r0}]
                     :opts {:type :group-opts}}]})))

(deftest test-reduce
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/reduce conj [] {:id 'r0 :fields '[value]})
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.pig/map->bind
                (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                  (clojure.core/eval
                    (quote (clojure.core/partial clojure.core/reduce conj [])))))
        :args [[[r0] value]]
        :requires [pigpen.join-test]
        :fields [value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :group
                     :id group1
                     :description "conj\n"
                     :keys [:pigpen.raw/group-all]
                     :join-types [:optional]
                     :fields [group [[r0] value]]
                     :field-type :frozen
                     :ancestors [{:fields [value], :id r0}]
                     :opts {:type :group-opts}}]})))

(deftest test-fold-fn
  (test-diff
    (pig/fold-fn +)
    '{:type :fold
      :init (clojure.core/require (quote [pigpen.pig]) (quote [pigpen.join-test]))
      :combinef (pigpen.pig/exec-combinef
                  (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                    (clojure.core/eval
                      (quote +))))
      :reducef (pigpen.pig/exec-reducef 0
                 (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                   (clojure.core/eval
                     (quote +))))
      :finalf (pigpen.pig/exec-finalf
                (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                  (clojure.core/eval (quote +)))
                (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                  (clojure.core/eval (quote clojure.core/identity))))}))

(deftest test-fold
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/fold + {:id 'r0 :fields '[value]})
      '{:type :generate
        :id generate2
        :description nil
        :fields [value]
        :field-type :frozen
        :opts {:type :generate-opts}
        :projections [{:type :projection-func
                       :alias value
                       :code {:type :code
                             :args [[[r0] value]]
                             :return "Algebraic"
                             :expr {:type :fold
                                    :init (clojure.core/require (quote [pigpen.pig]) (quote [pigpen.join-test]))
                                    :combinef (pigpen.pig/exec-combinef
                                                (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                  (clojure.core/eval (quote +))))
                                    :reducef (pigpen.pig/exec-reducef 0
                                               (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                 (clojure.core/eval (quote +))))
                                    :finalf (pigpen.pig/exec-finalf
                                              (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                (clojure.core/eval (quote +)))
                                              clojure.core/identity)}}}]
        :ancestors [{:type :group
                     :id group1
                     :description nil
                     :fields [group [[r0] value]]
                     :field-type :frozen
                     :join-types [:optional]
                     :keys [:pigpen.raw/group-all]
                     :opts {:type :group-opts}
                     :ancestors [{:fields [value], :id r0}]}]})))

(deftest test-cogroup
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/cogroup [({:fields ['value]} :by (fn [x] x) :fold (pig/fold-fn +))
                            ({:fields ['value]} :by (fn [y] y) :type :required :fold (pig/fold-fn +))]
                           (fn [_ x y] (* x y))
                           {:parallel 2})
      '{:type :bind
        :id bind11
        :description nil
        :func (pigpen.pig/map->bind
                (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                  (clojure.core/eval (quote (fn [_ x y] (* x y))))))
        :args [field5 field6 field7]
        :requires [pigpen.join-test]
        :fields [value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :generate
                     :id generate10
                     :description nil
                     :fields [field5 field6 field7]
                     :field-type :frozen
                     :opts {:type :generate-opts}
                     :projections [{:type :projection-field, :field group, :alias field5}
                                   {:type :projection-func
                                    :alias field6
                                    :code {:type :code
                                           :return "Algebraic"
                                           :args [[[generate2] value]]
                                           :expr {:type :fold
                                                  :init (clojure.core/require (quote [pigpen.pig]) (quote [pigpen.join-test]))
                                                  :combinef (pigpen.pig/exec-combinef
                                                              (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                                (clojure.core/eval (quote +))))
                                                  :reducef (pigpen.pig/exec-reducef 0
                                                             (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                               (clojure.core/eval (quote +))))
                                                  :finalf (pigpen.pig/exec-finalf
                                                            (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                              (clojure.core/eval (quote +)))
                                                            (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                              (clojure.core/eval (quote clojure.core/identity))))}}}
                                   {:type :projection-func
                                    :alias field7
                                    :code {:type :code
                                           :return "Algebraic"
                                           :args [[[generate4] value]]
                                           :expr {:type :fold
                                                  :init (clojure.core/require (quote [pigpen.pig]) (quote [pigpen.join-test]))
                                                  :combinef (pigpen.pig/exec-combinef
                                                              (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                                (clojure.core/eval (quote +))))
                                                  :reducef (pigpen.pig/exec-reducef 0
                                                             (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                               (clojure.core/eval (quote +))))
                                                  :finalf (pigpen.pig/exec-finalf
                                                            (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                              (clojure.core/eval (quote +)))
                                                            (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                              (clojure.core/eval (quote clojure.core/identity))))}}}]
                     :ancestors [{:type :group
                                  :id group9
                                  :description "(fn [_ x y] (* x y))\n"
                                  :fields [group [[generate2] key] [[generate2] value] [[generate4] key] [[generate4] value]]
                                  :field-type :frozen
                                  :join-types [:optional :required]
                                  :keys [[key] [key]]
                                  :opts {:type :group-opts
                                         :parallel 2}
                                  :ancestors [{:type :generate
                                               :id generate2
                                               :description nil
                                               :fields [key value]
                                               :field-type :frozen
                                               :opts {:type :generate-opts}
                                               :projections [{:type :projection-field, :field 0, :alias key}
                                                             {:type :projection-field, :field 1, :alias value}]
                                               :ancestors [{:type :bind
                                                            :id bind1
                                                            :description nil
                                                            :func (pigpen.pig/key-selector->bind
                                                                    (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                                      (clojure.core/eval (quote (fn [x] x)))))
                                                            :args [value]
                                                            :requires [pigpen.join-test]
                                                            :fields [value]
                                                            :field-type-in :frozen
                                                            :field-type-out :frozen-with-nils
                                                            :ancestors [{:fields [value]}]
                                                            :opts {:type :bind-opts
                                                                   :implicit-schema true}}]}
                                              {:type :generate
                                               :id generate4
                                               :description nil
                                               :fields [key value]
                                               :field-type :frozen
                                               :opts {:type :generate-opts}
                                               :projections [{:type :projection-field, :field 0, :alias key}
                                                             {:type :projection-field, :field 1, :alias value}]
                                               :ancestors [{:type :bind
                                                            :id bind3
                                                            :description nil
                                                            :func (pigpen.pig/key-selector->bind
                                                                    (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                                      (clojure.core/eval (quote (fn [y] y)))))
                                                            :args [value]
                                                            :requires [pigpen.join-test]
                                                            :fields [value]
                                                            :field-type-in :frozen
                                                            :field-type-out :frozen-with-nils
                                                            :ancestors [{:fields [value]}]
                                                            :opts {:type :bind-opts
                                                                   :implicit-schema true}}]}]}]}]})))

(deftest test-join
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/join [({:fields ['value]} :on (fn [x] x))
                         ({:fields ['value]} :on (fn [y] y) :type :optional)]
                        (fn [x y] (merge x y))
                        {:parallel 2})
      '{:type :bind
        :id bind6
        :description nil
        :func (pigpen.pig/map->bind
                (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                  (clojure.core/eval
                    (quote (fn [x y] (merge x y))))))
        :args [[[generate2 value]] [[generate4 value]]]
        :requires [pigpen.join-test]
        :fields [value]
        :field-type-out :frozen
        :field-type-in :frozen
        :opts {:type :bind-opts}
        :ancestors [{
                     :type :join
                     :id join5
                     :description "(fn [x y] (merge x y))\n"
                     :fields [[[generate2 key]] [[generate2 value]] [[generate4 key]] [[generate4 value]]]
                     :field-type :frozen
                     :join-types [:required :optional]
                     :keys [[key] [key]]
                     :opts {:type :join-opts
                            :parallel 2}
                     :ancestors [{:type :generate
                                  :id generate2
                                  :description nil
                                  :fields [key value]
                                  :field-type :frozen
                                  :opts {:type :generate-opts}
                                  :projections [{:type :projection-field, :field 0, :alias key}
                                                {:type :projection-field, :field 1, :alias value}]
                                  :ancestors [{:type :bind
                                               :id bind1
                                               :description nil
                                               :func (pigpen.pig/key-selector->bind
                                                       (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                         (clojure.core/eval (quote (fn [x] x)))))
                                               :args [value]
                                               :requires [pigpen.join-test]
                                               :fields [value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}]}
                                 {:type :generate
                                  :id generate4
                                  :description nil
                                  :fields [key value]
                                  :field-type :frozen
                                  :opts {:type :generate-opts}
                                  :projections [{:type :projection-field, :field 0, :alias key}
                                                {:type :projection-field, :field 1, :alias value}]
                                  :ancestors [{:type :bind
                                               :id bind3
                                               :description nil
                                               :func (pigpen.pig/key-selector->bind
                                                       (clojure.core/binding [clojure.core/*ns* (clojure.core/find-ns (quote pigpen.join-test))]
                                                         (clojure.core/eval (quote (fn [y] y)))))
                                               :args [value]
                                               :requires [pigpen.join-test]
                                               :fields [value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}]}]}]})))
