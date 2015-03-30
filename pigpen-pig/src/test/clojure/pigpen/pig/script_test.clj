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

(ns pigpen.pig.script-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.pig.script :refer :all]))

(use-fixtures :once schema.test/validate-schemas)

(deftest test-format-field
  (is (= (#'pigpen.pig.script/format-field "abc") "'abc'"))
  (is (= (#'pigpen.pig.script/format-field "a'b'c") "'a\\'b\\'c'"))
  (is (= (#'pigpen.pig.script/format-field 'r0/foo) "foo")))

(deftest test-expr->script
  (is (= (#'pigpen.pig.script/expr->script nil) nil))
  (is (= (#'pigpen.pig.script/expr->script "a'b\\c") "'a\\'b\\\\c'"))
  (is (= (#'pigpen.pig.script/expr->script 42) "42"))
  (is (= (#'pigpen.pig.script/expr->script 2147483648) "2147483648L"))
  (is (= (#'pigpen.pig.script/expr->script '?foo) "foo"))
  (is (= (#'pigpen.pig.script/expr->script '(clojure.core/let [foo '2] foo)) "2"))
  (is (= (#'pigpen.pig.script/expr->script '(clojure.core/let [foo '2] (and (= ?bar foo) (> ?baz 3)))) "((bar == 2) AND (baz > 3))")))

;; ********** Util **********

(deftest test-code
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (is (= ["DEFINE udf1 pigpen.PigPenFn('(require (quote [pigpen.runtime]))','identity');\n\n" "udf1()"]
           (command->script '{:type :code
                              :init (require '[pigpen.runtime])
                              :func identity
                              :udf :seq
                              :args []}
                            {})))))

(deftest test-register
  (is (= "REGISTER foo.jar;\n\n"
         (command->script '{:type :register
                            :jar "foo.jar"}
                          {}))))

(deftest test-option
  (is (= "SET pig.maxCombinedSplitSize 1000000;\n\n"
         (command->script '{:type :option
                            :option "pig.maxCombinedSplitSize"
                            :value 1000000}
                          {}))))

;; ********** IO **********

(deftest test-load
  (is (= "load0 = LOAD 'foo'\n    USING BinStorage();\n\n"
         (command->script '{:type :load
                            :id load0
                            :location "foo"
                            :storage :binary
                            :fields [load0/a load0/b load0/c]
                            :field-type :native
                            :opts {:type :load-opts}}
                          {})))
  (is (= "load0 = LOAD 'foo'
    USING PigStorage('\\n')
    AS (a:chararray, b:chararray, c:chararray);\n\n"
         (command->script '{:type :load
                            :id load0
                            :location "foo"
                            :storage :string
                            :fields [load0/a load0/b load0/c]
                            :field-type :native
                            :opts {:type :load-opts}}
                          {}))))

(deftest test-store
  (is (= "STORE relation0 INTO 'foo'\n    USING PigStorage();\n\n"
         (command->script '{:type :store
                            :id store0
                            :ancestors [relation0]
                            :location "foo"
                            :args [relation0/value]
                            :storage :string
                            :opts {:type :store-opts}}
                          {}))))

;; ********** Map **********

(deftest test-projection-field
  (is (= [nil "a AS (b)"]
         (command->script '{:type :projection
                            :expr {:type :field
                                   :field r0/a}
                            :flatten false
                            :alias [r1/b]}
                          {}))))

(deftest test-projection-func
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (is (= ["DEFINE udf1 pigpen.PigPenFn('','(fn [x] (* x x))');\n\n" "udf1('a', a) AS (b)"]
           (command->script '{:type :projection
                              :expr {:type :code
                                     :init nil
                                     :func (fn [x] (* x x))
                                     :udf :seq
                                     :args ["a" r0/a]}
                              :flatten false
                              :alias [r1/b]}
                            {})))))

(deftest test-project-flatten
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (is (= "DEFINE udf1 pigpen.PigPenFn('','(fn [x] [x x])');

project0 = FOREACH relation0 GENERATE
    FLATTEN(udf1('a', a)) AS (b);\n\n"
           (command->script '{:type :project
                              :id project0
                              :ancestors [relation0]
                              :fields [r1/b]
                              :field-type :frozen
                              :projections [{:type :projection
                                             :expr {:type :code
                                                    :init nil
                                                    :func (fn [x] [x x])
                                                    :udf :seq
                                                    :args ["a" r0/a]}
                                             :flatten true
                                             :alias [r1/b]}]}
                            {})))))

(deftest test-sort
  (is (= "sort0 = ORDER relation0 BY key ASC PARALLEL 10;\n\n"
         (command->script
           '{:type :sort
             :id sort0
             :description nil
             :ancestors [relation0]
             :fields [r0/key r0/value]
             :field-type :frozen
             :key r0/key
             :comp :asc
             :opts {:type :sort-opts
                    :parallel 10}}
           {}))))

(deftest test-rank
  (is (= "rank0 = RANK relation0 BY key ASC DENSE;\n\n"
         (command->script
           '{:type :rank
             :id rank0
             :description nil
             :ancestors [relation0]
             :fields [r0/key r0/value]
             :field-type :frozen
             :key r0/key
             :comp :asc
             :opts {:type :rank-opts
                    :dense true}}
           {})))

  (is (= "rank0 = RANK relation0;\n\n"
         (command->script
           '{:type :rank
             :id rank0
             :description nil
             :ancestors [relation0]
             :fields [r0/key r0/value]
             :field-type :frozen
             :opts {:type :rank-opts}}
           {}))))

;; ********** Filter **********

(deftest test-filter
  (is (= "filter0 = FILTER relation0 BY ((foo == 1) AND (bar > 2));\n\n"
         (command->script '{:type :filter
                            :id filter0
                            :fields [filter0/value]
                            :field-type :native
                            :ancestors [relation0]
                            :expr '(and (= ?foo 1) (> ?bar 2))}
                          {}))))

(deftest test-take
  (is (= "take0 = LIMIT relation0 100;\n\n"
         (command->script '{:type :take
                            :id take0
                            :fields [relation0/value]
                            :field-type :frozen
                            :ancestors [relation0]
                            :n 100
                            :opts {}}
                          {}))))

(deftest test-sample
  (is (= "sample0 = SAMPLE relation0 0.01;\n\n"
         (command->script '{:type :sample
                            :id sample0
                            :fields [relation0/value]
                            :field-type :frozen
                            :ancestors [relation0]
                            :p 0.01
                            :opts {}}
                          {}))))

;; ********** Join **********

(deftest test-reduce
  (is (= "group0 = COGROUP r0 ALL;\n\n"
         (command->script '{:type :reduce
                            :id group0
                            :fields [group0/value]
                            :field-type :frozen
                            :arg r0/value
                            :ancestors [r0]
                            :opts {:type :group-opts}}
                          {}))))

(deftest test-group
  (is (= "group0 = COGROUP r0 BY a;\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [r0/a]
                            :join-types [:optional]
                            :fields [group0/group r0/a]
                            :field-type :frozen
                            :field-dispatch :group
                            :ancestors [r0]
                            :opts {:type :group-opts}}
                          {})))

  (is (= "group0 = COGROUP r0 BY a, r1 BY b;\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [r0/a r1/b]
                            :join-types [:optional :optional]
                            :fields [group0/group r0/a r1/b]
                            :field-type :frozen
                            :field-dispatch :group
                            :ancestors [r0 r1]
                            :opts {:type :group-opts}}
                          {})))

  (is (= "group0 = COGROUP r0 BY a, r1 BY b USING 'merge' PARALLEL 2;\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [r0/a r1/b]
                            :join-types [:optional :optional]
                            :fields [group0/group r0/a r1/b]
                            :field-type :frozen
                            :field-dispatch :group
                            :ancestors [r0 r1]
                            :opts {:type :group-opts
                                   :strategy :merge
                                   :parallel 2}}
                          {})))

  (is (= "group0 = COGROUP r0 BY a INNER, r1 BY b INNER;\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [r0/a r1/b]
                            :join-types [:required :required]
                            :fields [group0/group r0/a r1/b]
                            :field-type :frozen
                            :field-dispatch :group
                            :ancestors [r0 r1]
                            :opts {:type :group-opts}}
                          {}))))

(deftest test-join
  (is (= "join0 = JOIN r0 BY a, r1 BY b;\n\n"
         (command->script '{:type :join
                            :id join0
                            :keys [r0/a r1/b]
                            :join-types [:required :required]
                            :fields [r0/a r1/b]
                            :field-type :frozen
                            :field-dispatch :join
                            :ancestors [r0 r1]
                            :opts {:type :join-opts}}
                          {})))

  (is (= "join0 = JOIN r0 BY a LEFT OUTER, r1 BY b USING 'replicated' PARALLEL 2;\n\n"
         (command->script '{:type :join
                            :id join0
                            :keys [r0/a r1/b]
                            :join-types [:required :optional]
                            :fields [r0/a r1/b]
                            :field-type :frozen
                            :field-dispatch :join
                            :ancestors [r0 r1]
                            :opts {:type :join-opts
                                   :strategy :replicated
                                   :parallel 2}}
                          {}))))

;; ********** Set **********

(deftest test-distinct
  (let [state {:partitioner (atom -1)}]
    (testing "normal"
      (is (= "distinct0 = DISTINCT relation0 PARALLEL 20;\n\n"
             (command->script '{:type :distinct
                                :id distinct0
                                :fields [relation0/value]
                                :field-type :frozen
                                :ancestors [relation0]
                                :opts {:type :distinct-opts
                                       :parallel 20}}
                              state))))

    (testing "with partitioner"
      (is (= "SET PigPenPartitioner0_type 'frozen';
SET PigPenPartitioner0_init '';
SET PigPenPartitioner0_func '(fn [n key] (mod (hash key) n))';

distinct0 = DISTINCT relation0 PARTITION BY pigpen.PigPenPartitioner.PigPenPartitioner0;\n\n"
             (command->script '{:type :distinct
                                :id distinct0
                                :fields [relation0/value]
                                :field-type :frozen
                                :ancestors [relation0]
                                :opts {:type :distinct-opts
                                       :partition-by (fn [n key] (mod (hash key) n))}}
                              state))))

    (testing "with native partitioner"
      (is (= "SET PigPenPartitioner1_type 'native';
SET PigPenPartitioner1_init '';
SET PigPenPartitioner1_func '(fn [n key] (mod (hash key) n))';

distinct0 = DISTINCT relation0 PARTITION BY pigpen.PigPenPartitioner.PigPenPartitioner1;\n\n"
             (command->script '{:type :distinct
                                :id distinct0
                                :fields [relation0/value]
                                :field-type :frozen
                                :ancestors [relation0]
                                :opts {:type :distinct-opts
                                       :partition-by (fn [n key] (mod (hash key) n))
                                       :partition-type :native}}
                              state))))))

(deftest test-concat
  (is (= "concat0 = UNION r0, r1;\n\n"
         (command->script '{:type :concat
                            :id concat0
                            :fields [r0/value]
                            :field-type :frozen
                            :ancestors [r0 r1]
                            :opts {:type :concat-opts}}
                          {}))))

;; ********** Script **********

;; TODO test-store-many
