(ns pigpen.pig.oven-test
  (:require [clojure.test :refer :all]
            [pigpen.raw :as raw]
            [pigpen.oven]
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
        (#'pigpen.oven/braise %)
        (#'pigpen.pig.oven/extract-references %)
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
        (#'pigpen.oven/braise %)
        (#'pigpen.pig.oven/extract-options %)
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
