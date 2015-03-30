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

(ns pigpen.pig.oven-test
  (:require [clojure.test :refer :all]
            [pigpen.raw :as raw]
            [pigpen.oven]
            [pigpen.io :as pig-io]
            [pigpen.map :as pig-map]
            [pigpen.pig.oven :as pig-oven]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]))

(defmethod pig-oven/storage->references :test-storage
  [_] ["ref.jar"])

(deftest test-extract-references
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (test-diff
      (->>
        (raw/load$ "foo" :test-storage '[foo] {})
        (raw/store$ "bar" :test-storage {})
        (#'pigpen.oven/braise {})
        (#'pigpen.pig.oven/extract-references {:extract-references? true})
        (map #(select-keys % [:type :id :ancestors :references :jar])))
      '[{:type :register
         :jar "ref.jar"}
        {:type :load
         :id load1}
        {:type :store
         :id store2
         :ancestors [load1]}])))

(deftest test-extract-options
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (test-diff
      (->>
        (raw/load$ "foo" :string '[foo] {:pig-options {"pig.maxCombinedSplitSize" 1000000}})
        (raw/store$ "bar" :string {})
        (#'pigpen.oven/braise {})
        (#'pigpen.pig.oven/extract-options {:extract-options? true})
        (map #(select-keys % [:type :id :ancestors :references :option :value])))
      '[{:type :option
         :option "pig.maxCombinedSplitSize"
         :value 1000000}
        {:type :load
         :id load1}
        {:type :store
         :id store2
         :ancestors [load1]}])))

(deftest test-merge-sort-rank
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig-io/return ["b" "c" "a"])
              (pig-map/sort)
              (pig-map/map-indexed vector))]

      (test-diff
        (->> s
          (#'pigpen.oven/braise {})
          (#'pigpen.pig.oven/merge-sort-rank {})
          (map #(select-keys % [:comp :key :ancestors :id :type])))
        '[{:type :return, :id return1}
          {:type :bind,   :id bind2,  :ancestors [return1]}
          {:type :sort,   :id sort3,  :ancestors [bind2], :key bind2/key, :comp :asc}
          {:type :rank,   :id rank4,  :ancestors [bind2], :key bind2/key, :comp :asc}
          {:type :bind,   :id bind5,  :ancestors [rank4]}]))))

(deftest test-expand-load-filters
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [command (raw/load$ "foo" :string '[foo] {:filter '(= foo 2)})]
      (test-diff (pig-oven/bake command)
                 '[{:type :register
                    :jar "pigpen.jar"}
                   {:type :load
                    :id load1_0
                    :description "foo"
                    :location "foo"
                    :fields [load1/foo]
                    :field-type :native
                    :storage :string
                    :opts {:type :load-opts
                           :filter (= foo 2)}}
                   {:type :filter
                    :id load1
                    :description nil
                    :ancestors [load1_0]
                    :expr (= foo 2)
                    :fields [load1/foo]
                    :field-type :native
                    :opts {:type :filter-opts}}]))))

(deftest test-clean
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig-io/return ["b" "c" "a"])
              (pig-map/sort)
              (pig-map/map-indexed vector))]

      (test-diff
        (->> s
          (#'pigpen.oven/braise {})
          (#'pigpen.pig.oven/merge-sort-rank {})
          (#'pigpen.oven/clean {})
          (map #(select-keys % [:comp :key :ancestors :id :type])))
        '[{:type :return, :id return1}
          {:type :bind,   :id bind2, :ancestors [return1]}
          {:type :rank,   :id rank4, :ancestors [bind2], :key bind2/key, :comp :asc}
          {:type :bind,   :id bind5, :ancestors [rank4]}]))))

(deftest test-dec-rank
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig-io/return [1 2 3])
              (pig-map/map-indexed vector))]

      (test-diff
        (->> s
          (#'pigpen.oven/braise {})
          (#'pigpen.pig.oven/dec-rank {})
          (#'pigpen.oven/optimize-binds {})
          (map #(select-keys % [:projections :ancestors :fields :id :type])))
        '[{:type :return, :id return1, :fields [return1/value]}
          {:type :rank, :id rank2_0, :fields [rank2/index rank2/value], :ancestors [return1]}
          {:type :project
           :id project4
           :fields [project4/value]
           :ancestors [rank2_0]
           :projections [{:type :projection
                          :expr {:type :code
                                 :init (clojure.core/require (quote [pigpen.runtime]))
                                 :func (clojure.core/comp (pigpen.runtime/process->bind (pigpen.runtime/pre-process nil :frozen))
                                                          (pigpen.runtime/process->bind (fn [[i v]] [(dec i) v]))
                                                          (pigpen.runtime/map->bind (pigpen.runtime/with-ns pigpen.pig.oven-test vector))
                                                          (pigpen.runtime/process->bind (pigpen.runtime/post-process nil :frozen)))
                                 :udf :seq
                                 :args [rank2_0/$0 rank2_0/value]}
                          :flatten true
                          :alias [project4/value]
                          :types nil}]}]))))

(deftest test-split-project
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig-io/return [1 2 3])
              (pig-map/map inc))]

      (test-diff
        (->> s
          (#'pigpen.oven/braise {})
          (#'pigpen.oven/optimize-binds {})
          (#'pigpen.pig.oven/split-project {})
          (map #(select-keys % [:projections :ancestors :fields :id :type])))
        '[{:type :return
           :id return1
           :fields [return1/value]}
          {:type :project
           :id project3_0
           :fields [project3_0/value0]
           :ancestors [return1]
           :projections [{:type :projection
                          :expr {:type :code
                                 :init (clojure.core/require (quote [pigpen.runtime]))
                                 :func (clojure.core/comp (pigpen.runtime/process->bind (pigpen.runtime/pre-process nil :frozen))
                                                          (pigpen.runtime/map->bind (pigpen.runtime/with-ns pigpen.pig.oven-test inc))
                                                          (pigpen.runtime/process->bind (pigpen.runtime/post-process nil :frozen)))
                                 :udf :seq
                                 :args [return1/value]}
                          :flatten false
                          :alias [project3_0/value0]}]}
          {:type :project
           :id project3
           :fields [project3/value]
           :ancestors [project3_0]
           :projections [{:type :projection
                          :expr {:type :field
                                 :field project3_0/value0}
                          :flatten true
                          :alias [project3/value]
                          :types nil}]}]))))
