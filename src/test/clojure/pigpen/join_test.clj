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

(deftest test-select?
  (is (#'pigpen.join/select? '({} on (fn [x] x))))
  (is (#'pigpen.join/select? '({} by (fn [x] x))))
  (is (#'pigpen.join/select? '(relation on (fn [x] x))))
  (is (#'pigpen.join/select? '(relation on f)))
  (is (not (#'pigpen.join/select? nil)))
  (is (not (#'pigpen.join/select? '[a on c])))
  (is (not (#'pigpen.join/select? (fn [x] x))))
  (is (not (#'pigpen.join/select? '({} foo (fn [x] x))))))

(deftest test-select->generate
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (test-diff
      (#'pigpen.join/select->generate true '[{:fields [value]} (fn [x] x)])
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
                     :requires []
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :frozen
                     :ancestors [{:fields [value]}]
                     :opts {:type :bind-opts
                            :implicit-schema true}}]})
    
    (test-diff
      (#'pigpen.join/select->generate false '[{:fields [value]} (fn [x] x)])
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
                     :requires []
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :frozen-with-nils
                     :ancestors [{:fields [value]}]
                     :opts {:type :bind-opts
                            :implicit-schema true}}]})))

(deftest test-split-selects
  
  (test-diff
    (#'pigpen.join/split-selects ['({:fields [value]} on (fn [x] x))
                                  '({:fields [value]} on (fn [y] y))
                                  'merge
                                  {:parallel 2}])
    '[[({:fields [value]} on (fn [x] x))
       ({:fields [value]} on (fn [y] y))]
      merge
      {:parallel 2}])
  
  (test-diff
    (#'pigpen.join/split-selects ['({:fields [value]} on (fn [x] x))
                                  '({:fields [value]} on (fn [y] y))
                                  'merge])
    '[[({:fields [value]} on (fn [x] x))
       ({:fields [value]} on (fn [y] y))]
      merge
      {}])
  
  (test-diff
    (#'pigpen.join/split-selects ['({:fields [value]} on (fn [x] x))
                                  '({:fields [value]} on (fn [y] y))
                                  {:a :b}
                                  {:parallel 2}])
    '[[({:fields [value]} on (fn [x] x))
       ({:fields [value]} on (fn [y] y))]
      {:a :b}
      {:parallel 2}])
  
  (test-diff
    (#'pigpen.join/split-selects ['({:fields [value]} on (fn [x] x))
                                  '({:fields [value]} on (fn [y] y))
                                  '(fn [x y] (* x y))
                                  {:parallel 2}])
    '[[({:fields [value]} on (fn [x] x))
       ({:fields [value]} on (fn [y] y))]
      (fn [x y] (* x y))
      {:parallel 2}]))

(deftest test-group-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/group-by (fn [v] (:foo v)) {:parallel 10} {:fields '[value]})
      '{:type :bind
        :id bind4
        :description nil
        :func (pigpen.pig/map->bind (clojure.core/fn [k v] (clojure.lang.MapEntry. k v)))
        :args [group [[generate2] value]]
        :requires []
        :fields [value]
        :field-type-out :frozen
        :field-type-in :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :group
                     :id group3
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
                                               :func (pigpen.pig/key-selector->bind (fn [v] (:foo v)))
                                               :args [value]
                                               :requires []
                                               :fields [value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}]}]}]})))

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
        :func (pigpen.pig/map->bind (clojure.core/partial clojure.core/reduce conj []))
        :args [[[r0] value]]
        :requires []
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

(deftest test-cogroup
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/cogroup ({:fields ['value]} by (fn [x] x))
                           ({:fields ['value]} by (fn [y] y) required)
                           (fn [_ x y] (* x y))
                           {:parallel 2})
      '{:type :bind
        :id bind6
        :description nil
        :func (pigpen.pig/map->bind (fn [_ x y] (* x y)))
        :args [group [[generate2] value] [[generate4] value]]
        :requires []
        :fields [value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :group
                     :id group5
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
                                               :func (pigpen.pig/key-selector->bind (fn [x] x))
                                               :args [value]
                                               :requires []
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
                                               :func (pigpen.pig/key-selector->bind (fn [y] y))
                                               :args [value]
                                               :requires []
                                               :fields [value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}]}]}]})))

(deftest test-join
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/join ({:fields ['value]} on (fn [x] x))
                        ({:fields ['value]} on (fn [y] y) optional)
                        (fn [x y] (merge x y))
                        {:parallel 2})
      '{:type :bind
        :id bind6
        :description nil
        :func (pigpen.pig/map->bind (fn [x y] (merge x y)))
        :args [[[generate2 value]] [[generate4 value]]]
        :requires []
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
                                               :func (pigpen.pig/key-selector->bind (fn [x] x))
                                               :args [value]
                                               :requires []
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
                                               :func (pigpen.pig/key-selector->bind (fn [y] y))
                                               :args [value]
                                               :requires []
                                               :fields [value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}]}]}]})))
