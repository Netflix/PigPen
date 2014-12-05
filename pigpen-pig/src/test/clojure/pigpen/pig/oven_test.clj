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
      (as-> nil %
        (raw/load$ "foo" '[foo] :test-storage {})
        (raw/store$ % "bar" :test-storage {})
        (#'pigpen.oven/braise % {})
        (#'pigpen.pig.oven/extract-references % {:extract-references? true})
        (map #(select-keys % [:type :id :ancestors :references :jar]) %))
      '[{:type :register
         :jar "ref.jar"}
        {:ancestors []
         :type :load
         :id load1}
        {:type :store
         :id store2
         :ancestors [load1]}])))

(deftest test-extract-options
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (test-diff
      (as-> nil %
        (raw/load$ "foo" '[foo] :string {:pig-options {"pig.maxCombinedSplitSize" 1000000}})
        (raw/store$ % "bar" :string {})
        (#'pigpen.oven/braise % {})
        (#'pigpen.pig.oven/extract-options % {:extract-options? true})
        (map #(select-keys % [:type :id :ancestors :references :option :value]) %))
      '[{:type :option
         :option "pig.maxCombinedSplitSize"
         :value 1000000}
        {:type :load
         :id load1
         :ancestors []}
        {:type :store
         :id store2
         :ancestors [load1]}])))

(deftest test-merge-order-rank
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig-io/return ["b" "c" "a"])
              (pig-map/sort)
              (pig-map/map-indexed vector))]

      (test-diff
        (as-> s %
          (#'pigpen.oven/braise % {})
          (#'pigpen.pig.oven/merge-order-rank % {})
          (map #(select-keys % [:type :id :ancestors :sort-keys]) %))
        '[{:type :return,   :id return1,   :ancestors []}
          {:type :bind,     :id bind2,     :ancestors [return1]}
          {:type :generate, :id generate3, :ancestors [bind2]}
          {:type :order,    :id order4,    :ancestors [generate3], :sort-keys [key :asc]}
          {:type :rank,     :id rank5,     :ancestors [generate3], :sort-keys [key :asc]}
          {:type :bind,     :id bind6,     :ancestors [rank5]}]))))

(deftest test-expand-load-filters
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [command (raw/load$ "foo" '[foo] :string {:filter '(= foo 2)})]
      (test-diff (pig-oven/bake command)
                 '[{:type :register
                    :id nil
                    :ancestors []
                    :fields []
                    :args []
                    :jar "pigpen.jar"}
                   {:type :load
                    :id load1_0
                    :description "foo"
                    :location "foo"
                    :ancestors []
                    :fields [foo]
                    :field-type :native
                    :args []
                    :storage :string
                    :opts {:type :load-opts
                           :filter (= foo 2)}}
                   {:type :filter
                    :id load1
                    :description nil
                    :ancestors [load1_0]
                    :expr (= foo 2)
                    :fields [foo]
                    :field-type :native
                    :args []
                    :opts {:type :filter-opts}}]))))

(deftest test-clean
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig-io/return ["b" "c" "a"])
              (pig-map/sort)
              (pig-map/map-indexed vector))]

      (test-diff
        (as-> s %
          (#'pigpen.oven/braise % {})
          (#'pigpen.pig.oven/merge-order-rank % {})
          (#'pigpen.oven/clean % {})
          (map #(select-keys % [:type :id :ancestors :sort-keys]) %))
        '[{:type :return,   :id return1,   :ancestors []}
          {:type :bind,     :id bind2,     :ancestors [return1]}
          {:type :generate, :id generate3, :ancestors [bind2]}
          {:type :rank,     :id rank5,     :ancestors [generate3], :sort-keys [key :asc]}
          {:type :bind,     :id bind6,     :ancestors [rank5]}]))))
