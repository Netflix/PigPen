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

(ns pigpen.raw-test
  (:use clojure.test
        pigpen.raw)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]))

(deftest test-field?

  (is (= true (#'pigpen.raw/field? 'foo)))
  (is (= false (#'pigpen.raw/field? nil)))
  (is (= false (#'pigpen.raw/field? [])))
  (is (= false (#'pigpen.raw/field? "foo")))
  (is (= false (#'pigpen.raw/field? :foo)))
  (is (= false (#'pigpen.raw/field? ["foo" :string]))))

;; ********** Util **********

(deftest test-register$

  (test-diff
    (register$ "foo")
    '{:type :register
      :jar "foo"})

  (is (thrown? AssertionError (register$ nil)))
  (is (thrown? AssertionError (register$ 123)))
  (is (thrown? AssertionError (register$ 'foo)))
  (is (thrown? AssertionError (register$ :foo))))

(deftest test-option$

  (test-diff
    (option$ "foo" 123)
    '{:type :option
      :option "foo"
      :value 123}))

(deftest test-code$
  (test-diff
    (code$ String ["a" 'b '[c d]]
           (expr$ '(require '[pigpen.pig])
                  '(var clojure.core/prn)))
    '{:type :code
      :return "String"
      :expr {:init (require (quote [pigpen.pig]))
             :func (var clojure.core/prn)}
      :args ["a" b [c d]]}))

;; ********** IO **********

(deftest test-storage$
  
  (test-diff
    (storage$ [] "foo" [])
    '{:type :storage
      :references []
      :func "foo"
      :args []})

  (test-diff
    (storage$ ["ref"] "foo" ["arg"])
    '{:type :storage
      :references ["ref"]
      :func "foo"
      :args ["arg"]})

  (is (thrown? AssertionError (storage$ nil nil nil)))
  (is (thrown? AssertionError (storage$ [] "" []))))

(deftest test-load$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (load$ "foo" ['value] default-storage {})
      '{:type :load
        :id load0
        :description "foo"
        :location "foo"
        :fields [value]
        :field-type :native
        :storage {:type :storage
                  :references []
                  :func "PigStorage", :args []}
        :opts {:type :load-opts}})))

(deftest test-store$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (store$ {} "foo" default-storage {})
      '{:type :store
        :id store0
        :description "foo"
        :ancestors [{}]
        :location "foo"
        :storage {:type :storage
                  :references []
                  :func "PigStorage"
                  :args []}
        :fields nil
        :opts {:type :store-opts}})))

(deftest test-return$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (return$ [{'value "foo"}] ['value])
      '{:type :return
        :id return0
        :fields [value]
        :data [{value "foo"}]})))

;; ********** Map **********

(deftest test-projection-field$
  (test-diff
    (projection-field$ 'value)
    '{:type :projection-field
      :field value
      :alias value}))

(deftest test-projection-func$
  (test-diff
    (projection-func$ 'value
                      (code$ String ['value]
                             (expr$ `(require '[pigpen.pig]) `identity)))
    '{:type :projection-func
      :code {:type :code
             :expr {:init (clojure.core/require (quote [pigpen.pig]))
                    :func clojure.core/identity}
             :return "String"
             :args [value]}
      :alias value}))

(deftest test-projection-flat$
  (test-diff
    (projection-flat$ 'value
                      (code$ String ['value]
                             (expr$ `(require '[pigpen.pig]) `identity)))
    '{:type :projection-flat
      :code {:type :code
             :expr {:init (clojure.core/require (quote [pigpen.pig]))
                    :func clojure.core/identity}
             :return "String"
             :args [value]}
      :alias value}))

(deftest test-generate$*
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (generate$* 'r0 [(projection-field$ 'value)] {})
      '{:type :generate
        :id generate0
        :description nil
        :ancestors [r0]
        :fields [value]
        :field-type :frozen
        :projections [{:type :projection-field
                       :field value
                       :alias value}]
        :opts {:type :generate-opts}})))

(deftest test-generate$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (generate$ {:fields ['value]} [(projection-field$ 'value)] {})
      '{:type :generate
        :id generate0
        :description nil
        :ancestors [{:fields [value]}]
        :fields [value]
        :field-type :frozen
        :projections [{:type :projection-field
                       :field value
                       :alias value}]
        :opts {:type :generate-opts}})))

(deftest test-bind$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (bind$ {:fields ['value]} '[pigpen.raw-test] `identity {})
      '{:type :bind
        :id bind0
        :description nil
        :ancestors [{:fields [value]}]
        :requires [pigpen.raw-test]
        :func clojure.core/identity
        :args [value]
        :fields [value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}})
    (test-diff
      (bind$ {:fields ['key 'value]} ['my-ns] `identity
             {:args ['key 'value]
              :alias 'val
              :field-type-in :native
              :field-type-out :native})
      '{:type :bind
        :id bind0
        :description nil
        :ancestors [{:fields [key value]}]
        :requires [my-ns]
        :func clojure.core/identity
        :args [key value]
        :fields [val]
        :field-type-in :native
        :field-type-out :native
        :opts {:type :bind-opts}})))

(deftest test-order$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (order$ {:fields ['key 'value]} ['key :asc] {})
      '{:type :order
        :id order0
        :description nil
        :ancestors [{:fields [key value]}]
        :fields [value]
        :field-type :frozen
        :sort-keys [key :asc]
        :opts {:type :order-opts}})))

(deftest test-rank$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (rank$ {:fields ['key 'value]} ['key :asc] {})
      '{:type :rank
        :id rank0
        :description nil
        :ancestors [{:fields [key value]}]
        :fields [key value $0]
        :field-type :frozen
        :sort-keys [key :asc]
        :opts {:type :rank-opts}})))

;; ********** Filter **********

(deftest test-filter$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (filter$ {:fields ['value]}
               (code$ String ['value]
                      (expr$ `(require '[pigpen.pig]) `identity))
               {})
      '{:type :filter
        :id filter0
        :description nil
        :ancestors [{:fields [value]}]
        :fields [value]
        :field-type :frozen
        :code {:type :code
               :expr {:init (clojure.core/require (quote [pigpen.pig]))
                      :func clojure.core/identity}
               :return "String"
               :args [value]}
        :opts {:type :filter-opts}})))

(deftest test-filter-native$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (filter-native$ {:fields ['value]}
                      '(and (= foo "a") (> bar 2))
                      {})
      '{:type :filter-native
        :id filter-native0
        :description nil
        :ancestors [{:fields [value]}]
        :fields [value]
        :field-type :native
        :expr (and (= foo "a") (> bar 2))
        :opts {:type :filter-native-opts}})))

(deftest test-distinct$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (distinct$ {} {:parallel 20})
      '{:type :distinct
        :id distinct0
        :description nil
        :ancestors [{}]
        :fields nil
        :field-type :frozen
        :opts {:type :distinct-opts
               :parallel 20}})))

(deftest test-limit$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (limit$ {} 1000 {})
      '{:type :limit
        :id limit0
        :description nil
        :ancestors [{}]
        :n 1000
        :fields nil
        :field-type :frozen
        :opts {:type :limit-opts}})))

(deftest test-sample$  
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (sample$ {} 0.001 {})
      '{:type :sample
        :id sample0
        :description nil
        :ancestors [{}]
        :p 0.0010
        :fields nil
        :field-type :frozen
        :opts {:type :sample-opts}})))

;; ********** Set **********

(deftest test-union$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (union$ '[{:id relation0, :fields [value]}
                {:id relation1, :fields [value]}] {})
      '{:type :union
        :id union0
        :description nil
        :fields [value]
        :field-type :frozen
        :ancestors [{:id relation0, :fields [value]}
                    {:id relation1, :fields [value]}]
        :opts {:type :union-opts}})))
 
;; ********** Join **********

(deftest test-group$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (group$ [{:id 'generate1, :fields '[key value]}
               {:id 'generate2, :fields '[key value]}]
              '[[key] [key]]
              [:optional :optional]
              {})
      '{:type :group
        :id group0
        :description nil
        :keys [[key] [key]]
        :join-types [:optional :optional]
        :fields [group [[generate1] key] [[generate1] value] [[generate2] key] [[generate2] value]]
        :field-type :frozen
        :ancestors [{:id generate1, :fields [key value]}
                    {:id generate2, :fields [key value]}]
        :opts {:type :group-opts}})))

(deftest test-join$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (join$ [{:id 'generate1, :fields '[key value]}
              {:id 'generate2, :fields '[key value]}]
             '[[key] [key]]
             [:required :required]
             {})
      '{:type :join
        :id join0
        :description nil
        :keys [[key] [key]]
        :join-types [:required :required]
        :fields [[[generate1 key]] [[generate1 value]] [[generate2 key]] [[generate2 value]]]
        :field-type :frozen
        :ancestors [{:id generate1, :fields [key value]}
                    {:id generate2, :fields [key value]}]
        :opts {:type :join-opts}})))

;; ********** Script **********

;; TODO test-script$
