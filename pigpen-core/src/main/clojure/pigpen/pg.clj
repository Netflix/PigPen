(ns pigpen.pg)

; Simple flow
(comment
  {:storage
    {:type       :storage,
     :references [],
     :func       "com.liveramp.pigpen_ext.pig.SequenceFileBucketStorage",
     :args       ["com.rapleaf.types.new_person_data.PIN"]},
   :location
           "/tmp/tests/liveramp.hadoop-test/CljTestCase_AUTOGEN/output",
   :fields [value],
   :ancestors
           [{:args           [value],
             :description    nil,
             :func           (pigpen.pig/map->bind (fn [v] (:pin v))),
             :fields         [value],
             :type           :bind,
             :field-type-out :native,
             :ancestors
                             [{:args        [value],
                               :description "(fn [{pin :pin}] (.is_set_email pin))\n",
                               :func
                                               (pigpen.pig/filter->bind
                                                 (pigpen.pig/with-ns
                                                   com.liveramp.pigpen-ext.example.example
                                                   (fn [{pin :pin}] (.is_set_email pin)))),
                               :fields         [value],
                               :type           :bind,
                               :field-type-out :frozen,
                               :ancestors
                                               [{:args        [abs_path rel_path obj],
                                                 :description nil,
                                                 :func
                                                                 (pigpen.pig/map->bind
                                                                   (fn
                                                                     [abs-path rel-path obj]
                                                                     {:absolute-path abs-path,
                                                                      :relative-path rel-path,
                                                                      (com.liveramp.pigpen_ext.PigPenUtil/fieldKeyword (class obj))
                                                                                     obj})),
                                                 :fields         [value],
                                                 :type           :bind,
                                                 :field-type-out :frozen,
                                                 :ancestors
                                                                 [{:type        :load,
                                                                   :id          load2692,
                                                                   :description "some/path",
                                                                   :location    "some/path",
                                                                   :fields      [abs_path rel_path obj],
                                                                   :field-type  :native,
                                                                   :storage
                                                                                {:type       :storage,
                                                                                 :references [],
                                                                                 :func
                                                                                             "com.liveramp.pigpen_ext.pig.SequenceFileBucketStorage",
                                                                                 :args       ["com.rapleaf.types.new_person_data.PIN"]},
                                                                   :opts        {:type :load-opts}}],
                                                 :requires       [],
                                                 :id             bind2693,
                                                 :opts           {:type :bind-opts},
                                                 :field-type-in  :native}],
                               :requires       [],
                               :id             bind2704,
                               :opts           {:type :bind-opts},
                               :field-type-in  :frozen}],
             :requires       [],
             :id             bind2705,
             :opts           {:type :bind-opts},
             :field-type-in  :frozen}],
   :type   :store,
   :id     store2706,
   :description
           "/tmp/tests/liveramp.hadoop-test/CljTestCase_AUTOGEN/output",
   :opts   {:type :store-opts}})

; Simple flow after baking
(comment
  [{:args [],
    :fields [],
    :ancestors [],
    :id nil,
    :type :register,
    :jar "pigpen.jar"}
   {:args [],
    :description "some/path",
    :field-type :native,
    :fields [abs_path rel_path obj],
    :type :load,
    :ancestors [],
    :id load6166,
    :opts {:type :load-opts},
    :storage
     {:type :storage,
      :references [],
      :func "com.liveramp.pigpen_ext.pig.SequenceFileBucketStorage",
      :args ["com.rapleaf.types.new_person_data.PIN"]},
    :location "some/path"}
   {:args [],
    :fields [value],
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
               :args [abs_path rel_path obj]},
       :alias value}],
    :type :generate,
    :id generate6186,
    :description "(fn [{pin :pin}] (.is_set_email pin))\n",
    :field-type :native,
    :opts {:type :generate-opts, :implicit-schema nil}}
   {:args [],
    :storage
          {:type :storage,
           :references [],
           :func "com.liveramp.pigpen_ext.pig.SequenceFileBucketStorage",
           :args ["com.rapleaf.types.new_person_data.PIN"]},
    :location
               "/tmp/tests/liveramp.hadoop-test/CljTestCase_AUTOGEN/output",
    :fields [value],
    :ancestors [generate6186],
    :type :store,
    :id store6180,
    :description
               "/tmp/tests/liveramp.hadoop-test/CljTestCase_AUTOGEN/output",
    :opts {:type :store-opts}}])
