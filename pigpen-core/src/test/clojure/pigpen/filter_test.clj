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

(ns pigpen.filter-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.filter :as pig]))

(deftest test-filter
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (let [^:local r0 {:fields '[r0/value]}]

      (test-diff
        (pig/filter (fn [{:keys [foo bar]}] (= foo bar)) r0)
        '{:type :bind
          :id bind0
          :description "(fn [{:keys [foo bar]}] (= foo bar))\n"
          :ancestors [{:fields [r0/value]}]
          :func (pigpen.runtime/filter->bind
                  (pigpen.runtime/with-ns pigpen.filter-test
                    (fn [{:keys [foo bar]}]
                      (= foo bar))))
          :args [r0/value]
          :requires []
          :fields [bind0/value]
          :field-type-in :frozen
          :field-type-out :frozen
          :opts {:type :bind-opts}})

      (is (thrown? AssertionError (pig/filter nil r0))))))

(deftest test-remove
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (let [^:local r0 {:fields '[r0/value]}]

      (test-diff
        (pig/remove (fn [{:keys [foo bar]}] (= foo bar)) r0)
        '{:type :bind
          :id bind0
          :description "(clojure.core/complement (fn [{:keys [foo bar]}] (= foo bar)))\n"
          :ancestors [{:fields [r0/value]}]
          :func (pigpen.runtime/filter->bind
                  (pigpen.runtime/with-ns pigpen.filter-test
                    (clojure.core/complement
                      (fn [{:keys [foo bar]}]
                        (= foo bar)))))
          :args [r0/value]
          :requires []
          :fields [bind0/value]
          :field-type-in :frozen
          :field-type-out :frozen
          :opts {:type :bind-opts}}))))

(deftest test-take
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (let [r0 {:fields '[r0/value]}]

      (test-diff
        (pig/take 2 r0)
        '{:type :limit
          :id limit0
          :description nil
          :ancestors [{:fields [r0/value]}]
          :n 2
          :fields [limit0/value]
          :field-type :frozen
          :opts {:type :limit-opts}}))))

(deftest test-sample
  (with-redefs [pigpen.raw/pigsym pigsym-zero]

    (let [r0 {:fields '[r0/value]}]

      (test-diff
        (pig/sample 0.01 r0)
        '{:type :sample
          :id sample0
          :description nil
          :ancestors [{:fields [r0/value]}]
          :p 0.01
          :fields [sample0/value]
          :field-type :frozen
          :opts {:type :sample-opts}}))))
