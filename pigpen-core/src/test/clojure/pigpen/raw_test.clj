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

(ns pigpen.raw-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [pigpen.raw :refer :all]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.join]
            [pigpen.set]))

(use-fixtures :once schema.test/validate-schemas)

(def r0
  '{:id r0
    :fields [r0/value]
    :field-type :frozen})

(def r1
  '{:id r1
    :fields [r1/key r1/value]
    :field-type :frozen})

;; ********** Util **********

(deftest test-code$
  (test-diff
    (code$ :seq
           '(require '[pigpen.runtime])
           '(var clojure.core/prn)
           ["a" 'r0/b 'c/d])
    '{:type :code
      :udf :seq
      :init (require (quote [pigpen.runtime]))
      :func (var clojure.core/prn)
      :args ["a" r0/b c/d]}))

;; ********** IO **********

(deftest test-load$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (load$ "foo" :string ['value] {})
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
      (store$ "foo" :string {} r0)
      '{:type :store
        :id store0
        :description "foo"
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :location "foo"
        :args [r0/value]
        :storage :string
        :opts {:type :store-opts}})))

(deftest test-return$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (return$ ['value] [{'value "foo"}])
      '{:type :return
        :id return0
        :field-type :frozen
        :fields [return0/value]
        :data [{return0/value "foo"}]})))

;; ********** Map **********

(deftest test-projection-field$
  (test-diff
    (projection-field$ 'r0/value)
    '{:type :projection
      :expr {:type :field
             :field r0/value}
      :flatten false
      :alias [value]}))

(deftest test-projection-func$
  (test-diff
    (projection-func$ '[value] true
                      (code$ :seq
                             `(require '[pigpen.runtime])
                             `identity
                             '[r0/value]))
    '{:type :projection
      :expr {:type :code
             :init (clojure.core/require (quote [pigpen.runtime]))
             :func clojure.core/identity
             :udf :seq
             :args [r0/value]}
      :flatten true
      :alias [value]}))

(deftest test-project$*
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (project$* [(projection-field$ 'r0/value)] {} 'r0)
      '{:type :project
        :id project0
        :description nil
        :ancestors [r0]
        :fields [project0/value]
        :field-type :frozen
        :projections [{:type :projection
                       :expr {:type :field
                              :field r0/value}
                       :flatten false
                       :alias [project0/value]}]
        :opts {:type :project-opts}})))

(deftest test-project$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (project$ [(projection-field$ 'r0/value)] {} r0)
      '{:type :project
        :id project0
        :description nil
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :fields [project0/value]
        :field-type :frozen
        :projections [{:type :projection
                       :expr {:type :field
                              :field r0/value}
                       :flatten false
                       :alias [project0/value]}]
        :opts {:type :project-opts}})))

(deftest test-bind$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (bind$ '[pigpen.raw-test] `identity {} r0)
      '{:type :bind
        :id bind0
        :description nil
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :requires [pigpen.raw-test]
        :func clojure.core/identity
        :args [r0/value]
        :fields [bind0/value]
        :field-type-in :frozen
        :field-type :frozen
        :types nil
        :opts {:type :bind-opts}})

    (test-diff
      (bind$ ['my-ns] `identity
             {:args '[r0/key r0/value]
              :alias '[val]
              :field-type-in :native
              :field-type :native}
              r1)
      '{:type :bind
        :id bind0
        :description nil
        :ancestors [{:id r1
                     :fields [r1/key r1/value]
                     :field-type :frozen}]
        :requires [my-ns]
        :func clojure.core/identity
        :args [r0/key r0/value]
        :fields [bind0/val]
        :field-type-in :native
        :field-type :native
        :types nil
        :opts {:type :bind-opts}})))

(deftest test-sort$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (sort$ 'key :asc {} r1)
      '{:type :sort
        :id sort0
        :description nil
        :ancestors [{:id r1
                     :fields [r1/key r1/value]
                     :field-type :frozen}]
        :fields [sort0/value]
        :field-type :frozen
        :key r1/key
        :comp :asc
        :opts {:type :sort-opts}})))

(deftest test-rank$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (rank$ {} r1)
      '{:type :rank
        :id rank0
        :description nil
        :ancestors [{:id r1
                     :fields [r1/key r1/value]
                     :field-type :frozen}]
        :fields [rank0/index rank0/key rank0/value]
        :field-type :frozen
        :opts {:type :rank-opts}})))

;; ********** Filter **********

(deftest test-filter$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (filter$ '(and (= foo "a") (> bar 2))
               {}
               r0)
      '{:type :filter
        :id filter0
        :description nil
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :fields [filter0/value]
        :field-type :native
        :expr (and (= foo "a") (> bar 2))
        :opts {:type :filter-opts}})))

(deftest test-take$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (take$ 1000 {} r0)
      '{:type :take
        :id take0
        :description nil
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :n 1000
        :fields [take0/value]
        :field-type :frozen
        :opts {:type :take-opts}})))

(deftest test-sample$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (sample$ 0.001 {} r0)
      '{:type :sample
        :id sample0
        :description nil
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :p 0.0010
        :fields [sample0/value]
        :field-type :frozen
        :opts {:type :sample-opts}})))

;; ********** Set **********

(deftest test-distinct$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (distinct$ {:parallel 20} r0)
      '{:type :distinct
        :id distinct0
        :description nil
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :fields [distinct0/value]
        :field-type :frozen
        :opts {:type :distinct-opts
               :parallel 20}})))

(deftest test-concat$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (concat$ {}
               '[{:id r0, :fields [r0/value], :field-type :frozen}
                 {:id r1, :fields [r1/value], :field-type :frozen}])
      '{:type :concat
        :id concat0
        :description nil
        :fields [concat0/value]
        :field-type :frozen
        :ancestors [{:id r0, :fields [r0/value], :field-type :frozen}
                    {:id r1, :fields [r1/value], :field-type :frozen}]
        :opts {:type :concat-opts}})))

;; ********** Join **********

(deftest test-reduce$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (reduce$ {} r0)
      '{:type :reduce
        :id reduce0
        :description nil
        :arg r0/value
        :fields [r0/value]
        :field-type :frozen
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :opts {:type :reduce-opts}})))

(deftest test-group$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (group$ :group
              [:optional :optional]
              {}
              [{:id 'g1, :fields '[g1/key g1/value], :field-type :frozen}
               {:id 'g2, :fields '[g2/key g2/value], :field-type :frozen}])
      '{:type :group
        :id group0
        :description nil
        :keys [g1/key g2/key]
        :join-types [:optional :optional]
        :field-dispatch :group
        :fields [group0/group g1/key g1/value g2/key g2/value]
        :field-type :frozen
        :ancestors [{:id g1, :fields [g1/key g1/value], :field-type :frozen}
                    {:id g2, :fields [g2/key g2/value], :field-type :frozen}]
        :opts {:type :group-opts}})))

(deftest test-join$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (join$ :join
             [:required :required]
             {}
             [{:id 'g1, :fields '[g1/key g1/value], :field-type :frozen}
              {:id 'g2, :fields '[g2/key g2/value], :field-type :frozen}])
      '{:type :join
        :id join0
        :description nil
        :keys [g1/key g2/key]
        :join-types [:required :required]
        :field-dispatch :join
        :fields [g1/key g1/value g2/key g2/value]
        :field-type :frozen
        :ancestors [{:id g1, :fields [g1/key g1/value], :field-type :frozen}
                    {:id g2, :fields [g2/key g2/value], :field-type :frozen}]
        :opts {:type :join-opts}})))

;; ********** Script **********

(deftest test-noop$
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (noop$ {} r0)
      '{:type :noop
        :id noop1
        :description nil
        :args [r0/value]
        :fields [noop1/value]
        :field-type :frozen
        :ancestors [{:id r0
                     :fields [r0/value]
                     :field-type :frozen}]
        :opts {:type :noop-opts}})))

(deftest test-store-many$
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (store-many$ [(store$ "foo" :string {} r0)
                    (store$ "foo" :string {} r0)])
      '{:type :store-many
        :id store-many3
        :ancestors [{:storage :string
                     :location "foo"
                     :args [r0/value]
                     :ancestors [{:id r0
                                  :fields [r0/value]
                                  :field-type :frozen}]
                     :type :store
                     :id store1
                     :description "foo"
                     :opts {:type :store-opts}}
                    {:storage :string
                     :location "foo"
                     :args [r0/value]
                     :ancestors [{:id r0
                                  :fields [r0/value]
                                  :field-type :frozen}]
                     :type :store
                     :id store2
                     :description "foo"
                     :opts {:type :store-opts}}]})))
