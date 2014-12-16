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

  (is (= true (#'pigpen.raw/field? 'foo/bar)))
  (is (= false (#'pigpen.raw/field? 'foo)))
  (is (= false (#'pigpen.raw/field? nil)))
  (is (= false (#'pigpen.raw/field? [])))
  (is (= false (#'pigpen.raw/field? "foo")))
  (is (= false (#'pigpen.raw/field? :foo)))
  (is (= false (#'pigpen.raw/field? ["foo" :string]))))

;; ********** Util **********

(deftest test-code$
  (test-diff
    (code$ :normal ["a" 'r0/b 'c/d]
           (expr$ '(require '[pigpen.runtime])
                  '(var clojure.core/prn)))
    '{:type :code
      :udf :normal
      :expr {:init (require (quote [pigpen.runtime]))
             :func (var clojure.core/prn)}
      :args ["a" r0/b c/d]}))

;; ********** IO **********

(deftest test-load$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (load$ "foo" ['value] :string {})
      '{:type :load
        :id load0
        :description "foo"
        :location "foo"
        :fields [load0/value]
        :field-type :native
        :storage :string
        :opts {:type :load-opts}})))

(deftest test-store$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (store$ {} "foo" :string {})
      '{:type :store
        :id store0
        :description "foo"
        :ancestors [{}]
        :location "foo"
        :storage :string
        :fields []
        :opts {:type :store-opts}})))

(deftest test-return$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (return$ [{'value "foo"}] ['value])
      '{:type :return
        :id return0
        :fields [return0/value]
        :data [{return0/value "foo"}]})))

;; ********** Map **********

(deftest test-projection-field$
  (test-diff
    (projection-field$ 'r0/value)
    '{:type :projection-field
      :field r0/value
      :alias [value]}))

(deftest test-projection-func$
  (test-diff
    (projection-func$ '[value]
                      (code$ :normal '[r0/value]
                             (expr$ `(require '[pigpen.runtime]) `identity)))
    '{:type :projection-func
      :code {:type :code
             :expr {:init (clojure.core/require (quote [pigpen.runtime]))
                    :func clojure.core/identity}
             :udf :normal
             :args [r0/value]}
      :alias [value]}))

(deftest test-projection-flat$
  (test-diff
    (projection-flat$ '[value]
                      (code$ :normal '[r0/value]
                             (expr$ `(require '[pigpen.runtime]) `identity)))
    '{:type :projection-flat
      :code {:type :code
             :expr {:init (clojure.core/require (quote [pigpen.runtime]))
                    :func clojure.core/identity}
             :udf :normal
             :args [r0/value]}
      :alias [value]}))

(deftest test-generate$*
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (generate$* 'r0 [(projection-field$ 'r0/value)] {})
      '{:type :generate
        :id generate0
        :description nil
        :ancestors [r0]
        :fields [generate0/value]
        :field-type :frozen
        :projections [{:type :projection-field
                       :field r0/value
                       :alias [generate0/value]}]
        :opts {:type :generate-opts}})))

(deftest test-generate$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (generate$ {:fields '[r0/value]} [(projection-field$ 'r0/value)] {})
      '{:type :generate
        :id generate0
        :description nil
        :ancestors [{:fields [r0/value]}]
        :fields [generate0/value]
        :field-type :frozen
        :projections [{:type :projection-field
                       :field r0/value
                       :alias [generate0/value]}]
        :opts {:type :generate-opts}})))

(deftest test-bind$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (bind$ {:fields '[r0/value]} '[pigpen.raw-test] `identity {})
      '{:type :bind
        :id bind0
        :description nil
        :ancestors [{:fields [r0/value]}]
        :requires [pigpen.raw-test]
        :func clojure.core/identity
        :args [r0/value]
        :fields [bind0/value]
        :field-type-in :frozen
        :field-type-out :frozen
        :opts {:type :bind-opts}})
    (test-diff
      (bind$ {:fields '[r0/key r0/value]} ['my-ns] `identity
             {:args '[r0/key r0/value]
              :alias '[val]
              :field-type-in :native
              :field-type-out :native})
      '{:type :bind
        :id bind0
        :description nil
        :ancestors [{:fields [r0/key r0/value]}]
        :requires [my-ns]
        :func clojure.core/identity
        :args [r0/key r0/value]
        :fields [bind0/val]
        :field-type-in :native
        :field-type-out :native
        :opts {:type :bind-opts}})))

(deftest test-order$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (order$ {:id 'r0, :fields '[r0/key r0/value]} 'key :asc {})
      '{:type :order
        :id order0
        :description nil
        :ancestors [{:id r0, :fields [r0/key r0/value]}]
        :fields [order0/value]
        :field-type :frozen
        :key r0/key
        :comp :asc
        :opts {:type :order-opts}})))

(deftest test-rank$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (rank$ {:fields '[r0/key r0/value]} {})
      '{:type :rank
        :id rank0
        :description nil
        :ancestors [{:fields [r0/key r0/value]}]
        :fields [rank0/index rank0/key rank0/value]
        :field-type :frozen
        :opts {:type :rank-opts}})))

;; ********** Filter **********

(deftest test-filter$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (filter$ {:fields '[r0/value]}
               '(and (= foo "a") (> bar 2))
               {})
      '{:type :filter
        :id filter0
        :description nil
        :ancestors [{:fields [r0/value]}]
        :fields [filter0/value]
        :field-type :native
        :expr (and (= foo "a") (> bar 2))
        :opts {:type :filter-opts}})))

(deftest test-limit$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (limit$ {} 1000 {})
      '{:type :limit
        :id limit0
        :description nil
        :ancestors [{}]
        :n 1000
        :fields []
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
        :fields []
        :field-type :frozen
        :opts {:type :sample-opts}})))

;; ********** Set **********

(deftest test-distinct$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (distinct$ {} {:parallel 20})
      '{:type :distinct
        :id distinct0
        :description nil
        :ancestors [{}]
        :fields []
        :field-type :frozen
        :opts {:type :distinct-opts
               :parallel 20}})))

(deftest test-union$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (union$ '[{:id r0, :fields [r0/value]}
                {:id r1, :fields [r1/value]}] {})
      '{:type :union
        :id union0
        :description nil
        :fields [union0/value]
        :field-type :frozen
        :ancestors [{:id r0, :fields [r0/value]}
                    {:id r1, :fields [r1/value]}]
        :opts {:type :union-opts}})))

;; ********** Join **********

(deftest test-reduce$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (reduce$ {:fields '[r0/value]} {})
      '{:type :reduce
        :id reduce0
        :description nil
        :value r0/value
        :fields [reduce0/value]
        :field-type :frozen
        :ancestors [{:fields [r0/value]}]
        :opts {:type :reduce-opts}})))

(deftest test-group$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (group$ [{:id 'g1, :fields '[g1/key g1/value]}
               {:id 'g2, :fields '[g2/key g2/value]}]
              :group
              [:optional :optional]
              {})
      '{:type :group
        :id group0
        :description nil
        :keys [g1/key g2/key]
        :join-types [:optional :optional]
        :field-dispatch :group
        :fields [group0/group g1/key g1/value g2/key g2/value]
        :field-type :frozen
        :ancestors [{:id g1, :fields [g1/key g1/value]}
                    {:id g2, :fields [g2/key g2/value]}]
        :opts {:type :group-opts}})))

(deftest test-join$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (join$ [{:id 'g1, :fields '[g1/key g1/value]}
              {:id 'g2, :fields '[g2/key g2/value]}]
             :join
             [:required :required]
             {})
      '{:type :join
        :id join0
        :description nil
        :keys [g1/key g2/key]
        :join-types [:required :required]
        :field-dispatch :join
        :fields [g1/key g1/value g2/key g2/value]
        :field-type :frozen
        :ancestors [{:id g1, :fields [g1/key g1/value]}
                    {:id g2, :fields [g2/key g2/value]}]
        :opts {:type :join-opts}})))

;; ********** Script **********

(deftest test-noop$
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (noop$ {:id 'r0, :fields '[r0/value]} {})
      '{:type :noop
        :id noop1
        :description nil
        :args [r0/value]
        :fields [noop1/value]
        :field-type :frozen
        :ancestors [{:fields [r0/value], :id r0}]
        :opts {:type :noop-opts}})))

(deftest test-script$
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (script$ [{:id 'r0, :fields '[r0/value]}
                {:id 'r1, :fields '[r1/value]}])
      '{:type :script
        :id script1
        :ancestors [{:fields [r0/value], :id r0}
                    {:fields [r1/value], :id r1}]})))
