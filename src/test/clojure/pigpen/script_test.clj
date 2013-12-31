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

(ns pigpen.script-test
  (:use clojure.test pigpen.script))

(deftest test-format-field
  (is (= (#'pigpen.script/format-field "abc") "'abc'"))
  (is (= (#'pigpen.script/format-field "a'b'c") "'a\\'b\\'c'"))
  (is (= (#'pigpen.script/format-field 'foo) "foo"))
  (is (= (#'pigpen.script/format-field '[[foo bar]]) "foo::bar"))
  (is (= (#'pigpen.script/format-field '[[foo bar baz]]) "foo::bar::baz"))
  (is (= (#'pigpen.script/format-field '[[foo] bar]) "foo.bar"))
  (is (= (#'pigpen.script/format-field '[[foo bar] baz]) "foo::bar.baz")))

(deftest test-expr->script
  (is (= (#'pigpen.script/expr->script nil) nil))
  (is (= (#'pigpen.script/expr->script "a'b\\c") "'a\\'b\\\\c'"))
  (is (= (#'pigpen.script/expr->script 42) "42"))
  (is (= (#'pigpen.script/expr->script 'foo) "foo"))
  (is (= (#'pigpen.script/expr->script '(clojure.core/let [foo '2] foo)) "2"))
  (is (= (#'pigpen.script/expr->script '(clojure.core/let [foo '2] (and (= bar foo) (> baz 3)))) "((bar == 2) AND (baz > 3))")))

;; ********** Util **********

(deftest test-code
  (is (= "pigpen.PigPenFnDataByteArray('(require (quote [pigpen.pig]))', 'identity')"
         (command->script '{:type :code
                            :expr {:init (require '[pigpen.pig])
                                   :func identity}
                            :return DataByteArray
                            :args []}))))

(deftest test-register
  (is (= "REGISTER foo.jar;\n\n"
         (command->script '{:type :register
                            :jar "foo.jar"}))))

;; ********** IO **********

(deftest test-storage
  (is (= "\n    USING PigStorage()"
         (command->script '{:type :storage
                            :func "PigStorage"
                            :args []})))
  (is (= "\n    USING PigStorage('foo', 'bar')"
         (command->script '{:type :storage
                            :func "PigStorage"
                            :args ["foo" "bar"]}))))

(deftest test-load
  (is (= "load0 = LOAD 'foo'\n    USING PigStorage()\n    AS (a, b, c);\n\n"
         (command->script '{:type :load
                            :id load0
                            :location "foo"
                            :storage {:type :storage
                                      :func "PigStorage"
                                      :args []}
                            :fields [a b c]
                            :opts {:type :load-opts}})))
  (is (= "load0 = LOAD 'foo'\n    USING PigStorage();\n\n"
         (command->script '{:type :load
                            :id load0
                            :location "foo"
                            :storage {:type :storage
                                      :func "PigStorage"
                                      :args []}
                            :fields [a b c]
                            :opts {:type :load-opts
                                   :implicit-schema true}})))
  (is (= "load0 = LOAD 'foo'
    USING PigStorage()
    AS (a:chararray, b:chararray, c:chararray);\n\n"
         (command->script '{:type :load
                            :id load0
                            :location "foo"
                            :storage {:type :storage
                                      :func "PigStorage"
                                      :args []}
                            :fields [a b c]
                            :opts {:type :load-opts
                                   :cast "chararray"}}))))

(deftest test-store
  (is (= "STORE relation0 INTO 'foo'\n    USING PigStorage();\n\n"
         (command->script '{:type :store
                            :id store0
                            :ancestors [relation0]
                            :location "foo"
                            :storage {:type :storage
                                      :func "PigStorage"
                                      :args []}
                            :opts {:type :store-opts}}))))

;; ********** Map **********

(deftest test-projection-field
  (is (= "a AS b"
         (command->script '{:type :projection-field
                            :field a
                            :alias b}))))

(deftest test-projection-func
  (is (= "pigpen.PigPenFnDataByteArray('', '(fn [x] (* x x))', 'a', a) AS b"
         (command->script '{:type :projection-func
                            :code {:type :code
                                   :expr {:init nil
                                          :func (fn [x] (* x x))}
                                   :return "DataByteArray"
                                   :args ["a" a]}
                            :alias b}))))

(deftest test-generate
  (is (= "generate0 = FOREACH relation0 GENERATE
    a AS b,
    pigpen.PigPenFnDataByteArray('', '(fn [x] (* x x))', 'a', a) AS b;\n\n"
         (command->script '{:type :generate
                            :id generate0
                            :ancestors [relation0]
                            :projections [{:type :projection-field
                                           :field a
                                           :alias b}
                                          {:type :projection-func
                                           :code {:type :code
                                                  :expr {:init nil
                                                         :func (fn [x] (* x x))}
                                                  :return "DataByteArray"
                                                  :args ["a" a]}
                                           :alias b}]}))))

(deftest test-generate-flatten
  (is (= "generate0 = FOREACH relation0 GENERATE
    FLATTEN(pigpen.PigPenFnDataBag('', '(fn [x] [x x])', 'a', a)) AS b;\n\n"
         (command->script '{:type :generate
                            :id generate0
                            :ancestors [relation0]
                            :projections [{:type :projection-flat
                                           :code {:type :code
                                                  :expr {:init nil
                                                         :func (fn [x] [x x])}
                                                  :return "DataBag"
                                                  :args ["a" a]}
                                           :alias b}]}))))

(deftest test-order
  (is (= "order0 = ORDER relation0 BY key1 ASC, key2 DESC PARALLEL 10;\n\n"
         (command->script
           '{:type :order
             :id order0
             :description nil
             :ancestors [relation0]
             :fields [key value]
             :field-type :frozen
             :sort-keys [key1 :asc key2 :desc]
             :opts {:type :order-opts
                    :parallel 10}}))))

(deftest test-rank
  (is (= "rank0 = RANK relation0 BY key ASC DENSE;\n\n"
         (command->script
           '{:type :rank
             :id rank0
             :description nil
             :ancestors [relation0]
             :fields [key value]
             :field-type :frozen
             :sort-keys [key :asc]
             :opts {:type :rank-opts
                    :dense true}})))
  
  (is (= "rank0 = RANK relation0;\n\n"
         (command->script
           '{:type :rank
             :id rank0
             :description nil
             :ancestors [relation0]
             :fields [key value]
             :field-type :frozen
             :sort-keys []
             :opts {:type :rank-opts}}))))

;; ********** Filter **********

(deftest test-filter
  (is (= "filter0 = FILTER relation0 BY pigpen.PigPenFnBoolean('', '(fn [x] (even? x))', 'a', a);\n\n"
         (command->script '{:type :filter
                            :id filter0
                            :ancestors [relation0]
                            :code {:type :code
                                   :expr {:init nil
                                          :func (fn [x] (even? x))}
                                   :return "Boolean"
                                   :args ["a" a]}}))))

(deftest test-filter-native
  (is (= "filter_native0 = FILTER relation0 BY ((foo == 1) AND (bar > 2));\n\n"
         (command->script '{:type :filter-native
                            :id filter-native0
                            :ancestors [relation0]
                            :expr '(and (= foo 1) (> bar 2))}))))

(deftest test-distinct
  (is (= "distinct0 = DISTINCT relation0 PARALLEL 20;\n\n"
         (command->script '{:type :distinct
                            :id distinct0
                            :ancestors [relation0]
                            :opts {:type :distinct-opts
                                   :parallel 20}}))))

(deftest test-limit
  (is (= "limit0 = LIMIT relation0 100;\n\n"
         (command->script '{:type :limit
                            :id limit0
                            :ancestors [relation0]
                            :n 100
                            :opts {:mode #{:script}}}))))

(deftest test-sample
  (is (= "sample0 = SAMPLE relation0 0.01;\n\n"
         (command->script '{:type :sample
                            :id sample0
                            :ancestors [relation0]
                            :p 0.01
                            :opts {:mode #{:script}}}))))

;; ********** Combine **********

(deftest test-union
  (is (= "union0 = UNION r0, r1;\n\n"
         (command->script '{:type :union
                            :id union0
                            :fields [value]
                            :ancestors [r0 r1]
                            :opts {:type :union-opts}}))))

(deftest test-group
  (is (= "group0 = COGROUP r0 BY (a);\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [[a]]
                            :join-types [:optional]
                            :ancestors [r0]
                            :opts {:type :group-opts}})))
  (is (= "group0 = COGROUP r0 BY (a, b);\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [[a b]]
                            :join-types [:optional]
                            :ancestors [r0]
                            :opts {:type :group-opts}})))
  (is (= "group0 = COGROUP r0 BY (a), r1 BY (b);\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [[a] [b]]
                            :join-types [:optional :optional]
                            :ancestors [r0 r1]
                            :opts {:type :group-opts}})))
  (is (= "group0 = COGROUP r0 BY (a), r1 BY (b) USING 'merge' PARALLEL 2;\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [[a] [b]]
                            :join-types [:optional :optional]
                            :ancestors [r0 r1]
                            :opts {:type :group-opts
                                   :strategy :merge
                                   :parallel 2}})))
  (is (= "group0 = COGROUP r0 BY (a) INNER, r1 BY (b) INNER;\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [[a] [b]]
                            :join-types [:required :required]
                            :ancestors [r0 r1]
                            :opts {:type :group-opts}})))
  (is (= "group0 = COGROUP r0 ALL;\n\n"
         (command->script '{:type :group
                            :id group0
                            :keys [:pigpen.raw/group-all]
                            :join-types [:optional]
                            :ancestors [r0]
                            :opts {:type :group-opts}}))))

(deftest test-join
  (is (= "join0 = JOIN r0 BY (a), r1 BY (b);\n\n"
         (command->script '{:type :join
                            :id join0
                            :keys [[a] [b]]
                            :join-types [:required :required]
                            :ancestors [r0 r1]
                            :opts {:type :group-opts}})))
  (is (= "join0 = JOIN r0 BY (a, b), r1 BY (b, c) USING 'replicated' PARALLEL 2;\n\n"
         (command->script '{:type :join
                            :id join0
                            :keys [[a b] [b c]]
                            :join-types [:required :required]
                            :ancestors [r0 r1]
                            :opts {:type :group-opts
                                   :strategy :replicated
                                   :parallel 2}})))
  (is (= "join0 = JOIN r0 BY (a, b) LEFT OUTER, r1 BY (b, c);\n\n"
         (command->script '{:type :join
                            :id join0
                            :keys [[a b] [b c]]
                            :join-types [:required :optional]
                            :ancestors [r0 r1]
                            :opts {:type :group-opts}}))))

;; ********** Script **********

;; TODO test-script
