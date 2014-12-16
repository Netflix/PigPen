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
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.join :as pig]
            [pigpen.fold :as fold]))

(deftest test-select->generate
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (test-diff
      (#'pigpen.join/select->generate {:join-nils true}
                                      '{:from {:fields [r0/value]}
                                        :key-selector (fn [x] x)})
      '{:type :bind
        :id bind0
        :description nil
        :func (pigpen.runtime/key-selector->bind (fn [x] x))
        :args [r0/value]
        :requires []
        :fields [bind0/key bind0/value]
        :field-type-in :frozen
        :field-type-out :frozen
        :ancestors [{:fields [r0/value]}]
        :opts {:type :bind-opts
               :implicit-schema true}})))

(deftest test-group-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/group-by (fn [v] (:foo v))
                    {:parallel 10
                     :fold (fold/fold-fn +)}
                    {:fields '[r0/value]})
      '{:type :bind
        :id bind4
        :description nil
        :func (pigpen.runtime/map->bind
                (pigpen.join/seq-groups
                  (clojure.core/fn [k v] (clojure.lang.MapEntry. k v))))
        :args [generate3/value0 generate3/value1]
        :requires [pigpen.join]
        :fields [bind4/value]
        :field-type-out :frozen
        :field-type-in :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :generate
                     :id generate3
                     :description nil
                     :projections [{:type :projection-field
                                    :field group2/group
                                    :alias [generate3/value0]}
                                   {:type :projection-func
                                    :alias [generate3/value1]
                                    :code {:type :code
                                           :args [bind1/value]
                                           :udf :algebraic
                                           :expr {:init ""
                                                  :func (pigpen.runtime/with-ns pigpen.join-test
                                                          (fold/fold-fn +))}}}]
                     :fields [generate3/value0 generate3/value1]
                     :field-type :frozen
                     :opts {:type :generate-opts}
                     :ancestors [{:type :group
                                  :id group2
                                  :description "(fn [v] (:foo v))\n"
                                  :field-dispatch :group
                                  :fields [group2/group bind1/key bind1/value]
                                  :field-type :frozen
                                  :join-types [:optional]
                                  :keys [bind1/key]
                                  :opts {:type :group-opts
                                         :parallel 10}
                                  :ancestors [{:type :bind
                                               :id bind1
                                               :description nil
                                               :func (pigpen.runtime/key-selector->bind
                                                       (pigpen.runtime/with-ns pigpen.join-test
                                                         (fn [v] (:foo v))))
                                               :args [r0/value]
                                               :requires []
                                               :fields [bind1/key bind1/value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [r0/value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}]}]}]})))

(deftest test-into
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/into [] {:id 'r0 :fields '[r0/value]})
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.runtime/map->bind (clojure.core/partial clojure.core/into []))
        :args [reduce1/value]
        :requires []
        :fields [bind2/value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :reduce
                     :id reduce1
                     :description "into []"
                     :value r0/value
                     :fields [reduce1/value]
                     :field-type :frozen
                     :ancestors [{:fields [r0/value], :id r0}]
                     :opts {:type :reduce-opts}}]})))

(deftest test-reduce
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/reduce conj [] {:id 'r0 :fields '[r0/value]})
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.runtime/map->bind
                (pigpen.runtime/with-ns pigpen.join-test
                  (clojure.core/partial clojure.core/reduce conj [])))
        :args [reduce1/value]
        :requires []
        :fields [bind2/value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :reduce
                     :id reduce1
                     :description "conj\n"
                     :value r0/value
                     :fields [reduce1/value]
                     :field-type :frozen
                     :ancestors [{:fields [r0/value], :id r0}]
                     :opts {:type :reduce-opts}}]})))

(deftest test-fold
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pig/fold + {:id 'r0 :fields '[r0/value]})
      '{:type :generate
        :id generate2
        :description nil
        :fields [generate2/value]
        :field-type :frozen
        :opts {:type :generate-opts}
        :projections [{:type :projection-func
                       :alias [generate2/value]
                       :code {:type :code
                             :args [reduce1/value]
                             :udf :algebraic
                             :expr {:init ""
                                    :func (pigpen.runtime/with-ns pigpen.join-test
                                            (pigpen.join/fold-fn* clojure.core/identity + + clojure.core/identity))}}}]
        :ancestors [{:type :reduce
                     :id reduce1
                     :description nil
                     :value r0/value
                     :fields [reduce1/value]
                     :field-type :frozen
                     :opts {:type :reduce-opts}
                     :ancestors [{:fields [r0/value], :id r0}]}]})))

(deftest test-cogroup
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/cogroup [({:fields ['r0/value]} :by (fn [x] x) :fold (pig/fold-fn +))
                            ({:fields ['r1/value]} :by (fn [y] y) :type :required :fold (pig/fold-fn +))]
                           (fn [_ x y] (* x y))
                           {:parallel 2})
      '{:type :bind
        :id bind5
        :description nil
        :func (pigpen.runtime/map->bind
                (pigpen.join/seq-groups
                  (pigpen.runtime/with-ns pigpen.join-test
                    (fn [_ x y] (* x y)))))
        :args [generate4/value0 generate4/value1 generate4/value2]
        :requires [pigpen.join]
        :fields [bind5/value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :generate
                     :id generate4
                     :description nil
                     :fields [generate4/value0 generate4/value1 generate4/value2]
                     :field-type :frozen
                     :opts {:type :generate-opts}
                     :projections [{:type :projection-field
                                    :field group3/group
                                    :alias [generate4/value0]}
                                   {:type :projection-func
                                    :alias [generate4/value1]
                                    :code {:type :code
                                           :udf :algebraic
                                           :args [bind1/value]
                                           :expr {:init ""
                                                  :func (pigpen.runtime/with-ns pigpen.join-test
                                                          (pig/fold-fn +))}}}
                                   {:type :projection-func
                                    :alias [generate4/value2]
                                    :code {:type :code
                                           :udf :algebraic
                                           :args [bind2/value]
                                           :expr {:init ""
                                                  :func (pigpen.runtime/with-ns pigpen.join-test
                                                          (pig/fold-fn +))}}}]
                     :ancestors [{:type :group
                                  :id group3
                                  :description "(fn [_ x y] (* x y))\n"
                                  :field-dispatch :group
                                  :fields [group3/group bind1/key bind1/value bind2/key bind2/value]
                                  :field-type :frozen
                                  :join-types [:optional :required]
                                  :keys [bind1/key bind2/key]
                                  :opts {:type :group-opts
                                         :parallel 2}
                                  :ancestors [{:type :bind
                                               :id bind1
                                               :description nil
                                               :func (pigpen.runtime/key-selector->bind
                                                       (pigpen.runtime/with-ns pigpen.join-test
                                                         (fn [x] x)))
                                               :args [r0/value]
                                               :requires []
                                               :fields [bind1/key bind1/value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [r0/value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}
                                              {:type :bind
                                               :id bind2
                                               :description nil
                                               :func (pigpen.runtime/key-selector->bind
                                                       (pigpen.runtime/with-ns pigpen.join-test
                                                         (fn [y] y)))
                                               :args [r1/value]
                                               :requires []
                                               :fields [bind2/key bind2/value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :ancestors [{:fields [r1/value]}]
                                               :opts {:type :bind-opts
                                                      :implicit-schema true}}]}]}]})))

(deftest test-join
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/join [({:fields ['r0/value]} :on (fn [x] x))
                         ({:fields ['r1/value]} :on (fn [y] y) :type :optional)]
                        (fn [x y] (merge x y))
                        {:parallel 2})
      '{:type :bind
        :id bind4
        :description nil
        :func (pigpen.runtime/map->bind
                (pigpen.runtime/with-ns pigpen.join-test
                  (fn [x y] (merge x y))))
        :args [bind1/value bind2/value]
        :requires []
        :fields [bind4/value]
        :field-type-out :frozen
        :field-type-in :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :join
                     :id join3
                     :description "(fn [x y] (merge x y))\n"
                     :field-dispatch :join
                     :fields [bind1/key bind1/value bind2/key bind2/value]
                     :field-type :frozen
                     :join-types [:required :optional]
                     :keys [bind1/key bind2/key]
                     :opts {:type :join-opts
                            :parallel 2}
                     :ancestors [{:type :bind
                                  :id bind1
                                  :description nil
                                  :func (pigpen.runtime/key-selector->bind
                                          (pigpen.runtime/with-ns pigpen.join-test
                                            (fn [x] x)))
                                  :args [r0/value]
                                  :requires []
                                  :fields [bind1/key bind1/value]
                                  :field-type-in :frozen
                                  :field-type-out :frozen-with-nils
                                  :ancestors [{:fields [r0/value]}]
                                  :opts {:type :bind-opts
                                         :implicit-schema true}}
                                 {:type :bind
                                  :id bind2
                                  :description nil
                                  :func (pigpen.runtime/key-selector->bind
                                          (pigpen.runtime/with-ns pigpen.join-test
                                            (fn [y] y)))
                                  :args [r1/value]
                                  :requires []
                                  :fields [bind2/key bind2/value]
                                  :field-type-in :frozen
                                  :field-type-out :frozen-with-nils
                                  :ancestors [{:fields [r1/value]}]
                                  :opts {:type :bind-opts
                                         :implicit-schema true}}]}]})))

(deftest test-filter-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/filter-by :key {:fields ['r0/value]} {:fields ['r1/value]})
      '{:type :bind
        :id bind4
        :description nil
        :func (pigpen.runtime/map->bind (clojure.core/fn [k v] v))
        :args [bind1/value bind2/value]
        :requires []
        :fields [bind4/value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :join
                     :id join3
                     :description ":key\n"
                     :field-dispatch :join
                     :fields [bind1/key bind1/value bind2/key bind2/value]
                     :field-type :frozen
                     :join-types [:required :required]
                     :keys [bind1/key bind2/key]
                     :opts {:type :join-opts
                            :sentinel-nil true}
                     :ancestors [{:type :bind
                                  :id bind1
                                  :description nil
                                  :func (pigpen.runtime/key-selector->bind
                                          (clojure.core/comp pigpen.runtime/sentinel-nil
                                                             clojure.core/identity))
                                  :args [r0/value]
                                  :requires []
                                  :fields [bind1/key bind1/value]
                                  :field-type-in :frozen
                                  :field-type-out :frozen-with-nils
                                  :opts {:type :bind-opts, :implicit-schema true}
                                  :ancestors [{:fields [r0/value]}]}
                                 {:type :bind
                                  :id bind2
                                  :description nil
                                  :func (pigpen.runtime/key-selector->bind
                                          (clojure.core/comp pigpen.runtime/sentinel-nil
                                                             (pigpen.runtime/with-ns pigpen.join-test :key)))
                                  :args [r1/value]
                                  :requires []
                                  :fields [bind2/key bind2/value]
                                  :field-type-in :frozen
                                  :field-type-out :frozen-with-nils
                                  :opts {:type :bind-opts, :implicit-schema true}
                                  :ancestors [{:fields [r1/value]}]}]}]})))

(deftest test-remove-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pigpen.join/remove-by :key {:fields ['r0/value]} {:fields ['r1/value]})
      '{:type :bind
        :id bind5
        :description nil
        :func (pigpen.runtime/mapcat->bind
                (fn [[k _ _ v]] (when (nil? k) [v])))
        :args [bind4/value]
        :requires []
        :fields [bind5/value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :bind
                     :id bind4
                     :description nil
                     :func (pigpen.runtime/map->bind clojure.core/vector)
                     :args [bind1/key bind1/value bind2/key bind2/value]
                     :requires []
                     :fields [bind4/value]
                     :field-type-in :frozen
                     :field-type-out :frozen
                     :opts {:type :bind-opts}
                     :ancestors [{:type :join
                                  :id join3
                                  :description ":key\n"
                                  :field-dispatch :join
                                  :fields [bind1/key bind1/value bind2/key bind2/value]
                                  :field-type :frozen
                                  :join-types [:optional :required]
                                  :keys [bind1/key bind2/key]
                                  :opts {:type :join-opts
                                         :all-args true
                                         :sentinel-nil true}
                                  :ancestors [{:type :bind
                                               :id bind1
                                               :description nil
                                               :func (pigpen.runtime/key-selector->bind
                                                       (clojure.core/comp pigpen.runtime/sentinel-nil
                                                                          clojure.core/identity))
                                               :args [r0/value]
                                               :requires []
                                               :fields [bind1/key bind1/value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :opts {:type :bind-opts, :implicit-schema true}
                                               :ancestors [{:fields [r0/value]}]}
                                              {:type :bind
                                               :id bind2
                                               :description nil
                                               :func (pigpen.runtime/key-selector->bind
                                                       (clojure.core/comp pigpen.runtime/sentinel-nil
                                                                          (pigpen.runtime/with-ns pigpen.join-test :key)))
                                               :args [r1/value]
                                               :requires []
                                               :fields [bind2/key bind2/value]
                                               :field-type-in :frozen
                                               :field-type-out :frozen-with-nils
                                               :opts {:type :bind-opts, :implicit-schema true}
                                               :ancestors [{:fields [r1/value]}]}]}]}]})))
