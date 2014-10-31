(ns pigpen.pg)

; Cogroup after baking
(comment
  [{:args        [],
    :description "/tmp/input2",
    :field-type  :native,
    :fields      [offset line],
    :type        :load,
    :ancestors   [],
    :id          load25,
    :opts        {:type :load-opts},
    :storage     :string,
    :location    "/tmp/input2"}
   {:args      [],
    :fields    [value],
    :ancestors [load25],
    :projections
                 [{:type :projection-flat,
                   :code
                          {:type :code,
                           :expr
                                 {:init (clojure.core/require '[pigpen.runtime]),
                                  :func
                                        (pigpen.runtime/exec
                                          [(pigpen.runtime/process->bind
                                             (pigpen.runtime/pre-process :cascading :native))
                                           (pigpen.runtime/map->bind
                                             (clojure.core/fn
                                               [offset line]
                                               (clojure.edn/read-string line)))
                                           (pigpen.runtime/key-selector->bind
                                             (pigpen.runtime/with-ns pigpen.cascading-test :c))
                                           (pigpen.runtime/process->bind
                                             (pigpen.runtime/post-process
                                               :cascading
                                               :frozen-with-nils))])},
                           :udf  :sequence,
                           :args [offset line]},
                   :alias value}],
    :type        :generate,
    :id          generate44,
    :description "",
    :field-type  :frozen-with-nils,
    :opts        {:type :generate-opts, :implicit-schema true}}
   {:args [],
    :projections
                 [{:type :projection-field, :field 0, :alias key}
                  {:type :projection-field, :field 1, :alias value}],
    :fields      [key value],
    :ancestors   [generate44],
    :type        :generate,
    :id          generate30,
    :description nil,
    :field-type  :frozen,
    :opts        {:type :generate-opts}}
   {:args        [],
    :description "/tmp/input1",
    :field-type  :native,
    :fields      [offset line],
    :type        :load,
    :ancestors   [],
    :id          load23,
    :opts        {:type :load-opts},
    :storage     :string,
    :location    "/tmp/input1"}
   {:args      [],
    :fields    [value],
    :ancestors [load23],
    :projections
                 [{:type :projection-flat,
                   :code
                          {:type :code,
                           :expr
                                 {:init (clojure.core/require '[pigpen.runtime]),
                                  :func
                                        (pigpen.runtime/exec
                                          [(pigpen.runtime/process->bind
                                             (pigpen.runtime/pre-process :cascading :native))
                                           (pigpen.runtime/map->bind
                                             (clojure.core/fn
                                               [offset line]
                                               (clojure.edn/read-string line)))
                                           (pigpen.runtime/key-selector->bind
                                             (pigpen.runtime/with-ns pigpen.cascading-test :a))
                                           (pigpen.runtime/process->bind
                                             (pigpen.runtime/post-process
                                               :cascading
                                               :frozen-with-nils))])},
                           :udf  :sequence,
                           :args [offset line]},
                   :alias value}],
    :type        :generate,
    :id          generate45,
    :description "",
    :field-type  :frozen-with-nils,
    :opts        {:type :generate-opts, :implicit-schema true}}
   {:args [],
    :projections
                 [{:type :projection-field, :field 0, :alias key}
                  {:type :projection-field, :field 1, :alias value}],
    :fields      [key value],
    :ancestors   [generate45],
    :type        :generate,
    :id          generate28,
    :description nil,
    :field-type  :frozen,
    :opts        {:type :generate-opts}}
   {:args        [],
    :description "(fn [k l r] [k (map :b l) (map :d r)])\n",
    :field-type  :frozen,
    :fields
                 [group
                  [[generate28] key]
                  [[generate28] value]
                  [[generate30] key]
                  [[generate30] value]],
    :type        :group,
    :ancestors   [generate28 generate30],
    :keys        [[key] [key]],
    :join-types  [:optional :optional],
    :id          group39,
    :opts        {:type :group-opts}}
   {:args [],
    :projections
                 [{:type :projection-field, :field group, :alias value0}
                  {:type  :projection-field,
                   :field [[generate28] value],
                   :alias value1}
                  {:type  :projection-field,
                   :field [[generate30] value],
                   :alias value2}],
    :fields      [value0 value1 value2],
    :ancestors   [group39],
    :type        :generate,
    :id          generate40,
    :description nil,
    :field-type  :frozen,
    :opts        {:type :generate-opts}}
   {:args      [],
    :fields    [value],
    :ancestors [generate40],
    :projections
                 [{:type :projection-flat,
                   :code
                          {:type :code,
                           :expr
                                 {:init (clojure.core/require '[pigpen.runtime]),
                                  :func
                                        (pigpen.runtime/exec
                                          [(pigpen.runtime/process->bind
                                             (pigpen.runtime/pre-process :cascading :frozen))
                                           (pigpen.runtime/map->bind
                                             (pigpen.runtime/with-ns
                                               pigpen.cascading-test
                                               (fn [k l r] [k (map :b l) (map :d r)])))
                                           (pigpen.runtime/map->bind clojure.core/pr-str)
                                           (pigpen.runtime/process->bind
                                             (pigpen.runtime/post-process :cascading :native))])},
                           :udf  :sequence,
                           :args [value0 value1 value2]},
                   :alias value}],
    :type        :generate,
    :id          generate46,
    :description "",
    :field-type  :native,
    :opts        {:type :generate-opts, :implicit-schema nil}}
   {:args        [],
    :storage     :string,
    :location    "/tmp/output",
    :fields      [value],
    :ancestors   [generate46],
    :type        :store,
    :id          store43,
    :description "/tmp/output",
    :opts        {:type :store-opts}}])

; cogroup script
(comment "REGISTER pigpen.jar;

load105 = LOAD '/tmp/input2'
    USING text()
    AS (offset, line);

DEFINE udf128 pigpen.PigPenFnDataBag('(clojure.core/require (quote [pigpen.pig]))','(pigpen.pig/exec [(pigpen.pig/pre-process :native)
 (pigpen.pig/map->bind (clojure.core/fn [offset line] (clojure.edn/read-string line)))
  (pigpen.pig/key-selector->bind (pigpen.pig/with-ns pigpen.cascading-test :c)) (pigpen.pig/post-process :frozen-with-nils)])');

generate125 = FOREACH load105 GENERATE
    FLATTEN(udf128(offset, line));

generate110 = FOREACH generate125 GENERATE
    $0 AS key,
    $1 AS value;

load103 = LOAD '/tmp/input1'
    USING text()
    AS (offset, line);

DEFINE udf129 pigpen.PigPenFnDataBag('(clojure.core/require (quote [pigpen.pig]))','(pigpen.pig/exec [(pigpen.pig/pre-process :native)
 (pigpen.pig/map->bind (clojure.core/fn [offset line] (clojure.edn/read-string line)))
 (pigpen.pig/key-selector->bind (pigpen.pig/with-ns pigpen.cascading-test :a)) (pigpen.pig/post-process :frozen-with-nils)])');

generate126 = FOREACH load103 GENERATE
    FLATTEN(udf129(offset, line));

generate108 = FOREACH generate126 GENERATE
    $0 AS key,
    $1 AS value;

group119 = COGROUP generate108 BY (key), generate110 BY (key);

generate120 = FOREACH group119 GENERATE
    group AS value0,
    generate108.value AS value1,
    generate110.value AS value2;

DEFINE udf130 pigpen.PigPenFnDataBag('(clojure.core/require (quote [pigpen.pig]))','(pigpen.pig/exec [(pigpen.pig/pre-process :frozen)
 (pigpen.pig/map->bind (pigpen.pig/with-ns pigpen.cascading-test (fn [k l r] [k (map :b l) (map :d r)]))) (pigpen.pig/post-process :frozen)])');

generate127 = FOREACH generate120 GENERATE
    FLATTEN(udf130(value0, value1, value2)) AS value;")

; my-func from the example after baking
(comment
  [{:args      [],
    :fields    [],
    :ancestors [],
    :id        nil,
    :type      :register,
    :jar       "pigpen.jar"}
   {:args        [],
    :description "input",
    :field-type  :native,
    :fields      [value],
    :type        :load,
    :ancestors   [],
    :id          load3596,
    :opts        {:type :load-opts, :cast "chararray"},
    :storage
                 {:type       :storage,
                  :references [],
                  :func       "PigStorage",
                  :args       ["\\u0000"]},
    :location    "input"}
   {:args      [],
    :fields    [value],
    :ancestors [load3596],
    :projections
               [{:type :projection-flat,
                 :code
                        {:type :code,
                         :expr
                                 {:init
                                   (clojure.core/require '[pigpen.pig] '[pigpen.extensions.core]),
                                  :func
                                   (pigpen.pig/exec
                                     [(pigpen.pig/pre-process :native)
                                      (pigpen.pig/map->bind
                                        (clojure.core/fn
                                          [s]
                                          (if s (pigpen.extensions.core/structured-split s "\t"))))
                                      (pigpen.pig/map->bind
                                        (fn
                                          [[a b c]]
                                          {:sum (+ (Integer/valueOf a) (Integer/valueOf b)), :name c}))
                                      (pigpen.pig/filter->bind (fn [{:keys [sum]}] (< sum 5)))
                                      (pigpen.pig/map->bind clojure.core/pr-str)
                                      (pigpen.pig/post-process :native)])},
                         :return "DataBag",
                         :args   [value]},
                 :alias value}],
    :type      :generate,
    :id        generate3614,
    :description
                "(fn\n [[a b c]]\n {:sum (+ (Integer/valueOf a) (Integer/valueOf b)), :name c})\n(fn [{:keys [sum]}] (< sum 5))\n",
    :field-type :native,
    :opts       {:type :generate-opts, :implicit-schema nil}}
   {:args [],
    :storage
                 {:type :storage, :references [], :func "PigStorage", :args []},
    :location    "output",
    :fields      [value],
    :ancestors   [generate3614],
    :type        :store,
    :id          store3613,
    :description "output",
    :opts        {:type :store-opts}}])

; my-func script
(comment "REGISTER pigpen.jar;

load23 = LOAD '/tmp/input'
    USING text()
    AS (offset, line);

DEFINE udf50 pigpen.PigPenFnDataBag('(clojure.core/require (quote [pigpen.pig]))','(pigpen.pig/exec [(pigpen.pig/pre-process :native)
(pigpen.pig/map->bind (clojure.core/fn [offset line] (pigpen.extensions.core/structured-split line " \\ t ")))
(pigpen.pig/map->bind (pigpen.pig/with-ns pigpen.cascading-test (fn [[a b c]] {:sum (+ (Integer/valueOf a) (Integer/valueOf b)), :name c})))
(pigpen.pig/filter->bind (pigpen.pig/with-ns pigpen.cascading-test (fn [{:keys [sum]}] (< sum 5)))) (pigpen.pig/map->bind clojure.core/pr-str)
(pigpen.pig/post-process :native)])');

generate49 = FOREACH load23 GENERATE
    FLATTEN(udf50(offset, line)) AS value;

STORE generate49 INTO '/tmp/output'
    USING text();")

; Simple flow after baking
(comment
  [{:args      [],
    :fields    [],
    :ancestors [],
    :id        nil,
    :type      :register,
    :jar       "pigpen.jar"}
   {:args        [],
    :description "some/path",
    :field-type  :native,
    :fields      [abs_path rel_path obj],
    :type        :load,
    :ancestors   [],
    :id          load6166,
    :opts        {:type :load-opts},
    :storage
                 {:type       :storage,
                  :references [],
                  :func       "com.liveramp.pigpen_ext.pig.SequenceFileBucketStorage",
                  :args       ["com.rapleaf.types.new_person_data.PIN"]},
    :location    "some/path"}
   {:args      [],
    :fields    [value],
    :ancestors [load6166],
    :projections
                 [{:type :projection-flat,
                   :code
                          {:type :code,
                           :expr
                                   {:init (clojure.core/require '[pigpen.pig]),
                                    :func
                                          (pigpen.pig/exec
                                            [(pigpen.pig/pre-process :native)
                                             (pigpen.pig/map->bind
                                               (fn
                                                 [abs-path rel-path obj]
                                                 {:absolute-path abs-path,
                                                  :relative-path rel-path,
                                                  (com.liveramp.pigpen_ext.PigPenUtil/fieldKeyword
                                                    (class obj))
                                                                 obj}))
                                             (pigpen.pig/filter->bind
                                               (pigpen.pig/with-ns
                                                 com.liveramp.pigpen-ext.example.example
                                                 (fn [{pin :pin}] (.is_set_email pin))))
                                             (pigpen.pig/map->bind (fn [v] (:pin v)))
                                             (pigpen.pig/post-process :native)])},
                           :return "DataBag",
                           :args   [abs_path rel_path obj]},
                   :alias value}],
    :type        :generate,
    :id          generate6186,
    :description "(fn [{pin :pin}] (.is_set_email pin))\n",
    :field-type  :native,
    :opts        {:type :generate-opts, :implicit-schema nil}}
   {:args [],
    :storage
          {:type       :storage,
           :references [],
           :func       "com.liveramp.pigpen_ext.pig.SequenceFileBucketStorage",
           :args       ["com.rapleaf.types.new_person_data.PIN"]},
    :location
               "/tmp/tests/liveramp.hadoop-test/CljTestCase_AUTOGEN/output",
    :fields    [value],
    :ancestors [generate6186],
    :type      :store,
    :id        store6180,
    :description
               "/tmp/tests/liveramp.hadoop-test/CljTestCase_AUTOGEN/output",
    :opts      {:type :store-opts}}])
