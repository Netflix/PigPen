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

(ns pigpen.parquet-test
  (:require [clojure.test :refer :all]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc regex->string]]
            [pigpen.parquet :as pq]))

(deftest test-load-parquet
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pq/load-parquet "foo" {:x :chararray, :y :int})
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.runtime/map->bind (pigpen.runtime/args->map pigpen.runtime/native->clojure))
        :args ["y" load1/y "x" load1/x]
        :requires []
        :fields [bind2/value]
        :field-type-in :native
        :field-type :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :load
                     :id load1
                     :description "foo"
                     :location "foo"
                     :fields [load1/y load1/x]
                     :field-type :native
                     :storage :parquet
                     :opts {:type :load-opts
                            :schema "y:int,x:chararray"}}]})))

(deftest test-store-parquet
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (pq/store-parquet "foo" {:x :chararray, :y :int} {:fields '[value]})
      '{:type :store
        :id store2
        :description "foo"
        :storage :parquet
        :location "foo"
        :args [bind1/y bind1/x]
        :ancestors [{:type :bind
                     :id bind1
                     :description nil
                     :func (pigpen.runtime/keyword-field-selector->bind [:y :x])
                     :args [value]
                     :requires []
                     :fields [bind1/y bind1/x]
                     :field-type-in :frozen
                     :field-type :frozen
                     :opts {:type :bind-opts
                            :field-type-out :native}
                     :ancestors [{:fields [value]}]}]
        :opts {:type :store-opts
               :schema {:y :int, :x :chararray}}})))
