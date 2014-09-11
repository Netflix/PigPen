(ns pigpen.cascading-test
  (:import (java.io File)
           (org.apache.hadoop.fs FileSystem Path)
           (org.apache.hadoop.conf Configuration))
  (:use clojure.test pigpen.cascading)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]))

(deftest test-taps
  (println (command->flowdef '{:type     :load
                               :id       load0
                               :location "some/path"
                               :storage  {:type :storage
                                          :func "text"
                                          :args []}
                               :fields   [a b c]
                               :opts     {:type :load-opts}}
                             {}))
  (is (= 1 1)))

(deftest test-load-text
  (let [load-cmd (load-text "the/location")]
    (println "load-cmd:" load-cmd)
    (println (command->flowdef load-cmd {}))))

(def commands '[{:args      [],
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
                              :func       "text",
                              :args       ["\\u0000"]},
                :location    "/tmp/input"}
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
                             {:type :storage, :references [], :func "text", :args []},
                :location    "/tmp/output",
                :fields      [value],
                :ancestors   [generate3614],
                :type        :store,
                :id          store3613,
                :description "output",
                :opts        {:type :store-opts}}])

(deftest test-commands->flow
  (spit "/tmp/input" "1\t2\tfoo\n4\t5\tbar")
  (.delete (FileSystem/get (Configuration.)) (Path. "/tmp/output") true)
  (let [flow (commands->flow commands)]
    (println flow)
    (.complete flow)))

;(run-tests 'pigpen.cascading-test)
