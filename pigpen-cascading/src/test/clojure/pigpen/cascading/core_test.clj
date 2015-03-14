(ns pigpen.cascading.core-test
  (:import (java.io File)
           (org.apache.hadoop.fs FileSystem Path)
           (org.apache.hadoop.conf Configuration)
           (cascading.scheme.hadoop TextLine)
           (cascading.tap.hadoop Hfs)
           (cascading.tuple Fields Tuple TupleEntry TupleEntryCollector)
           (cascading.operation FunctionCall)
           (pigpen.cascading PigPenFunction))
  (:require [clojure.test :refer :all]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.core :as pigpen]
            [pigpen.cascading :as cascading]
            [pigpen.cascading.runtime :as runtime]
            [pigpen.oven :as oven]
            [clojure.pprint :as pp]
            [criterium.core :as criterium]))

(def input1 "/tmp/input1")
(def input2 "/tmp/input2")
(def input3 "/tmp/input3")
(def output1 "/tmp/output1")
(def output2 "/tmp/output2")

(defn setup [fn]
  (.delete (FileSystem/get (Configuration.)) (Path. output1) true)
  (.delete (FileSystem/get (Configuration.)) (Path. output2) true)
  (fn))

(clojure.test/use-fixtures :each setup)

(defn write-input [path objs]
  (spit path (clojure.string/join (map prn-str objs))))

(defn read-output [path]
  (map read-string (clojure.string/split (slurp (str path "/part-00000")) #"\n")))

(deftest test-load-tap
  (write-input input1 ["line1" "line2"])
  (let [tap (Hfs. (TextLine. (Fields. (into-array (map str ["line"])))) input1)
        cmd (->> (cascading/load-tap tap)
                 (pigpen/store-tsv output1))]
    (-> cmd (cascading/generate-flow) (.complete))
    (is (= ["line1" "line2"] (read-output output1)))))

(deftest test-load-tap-custom-fn
  (write-input input1 ["line1" "line2"])
  (let [tap (Hfs. (TextLine. (Fields. (into-array (map str ["offset" "line"])))) input1)
        cmd (->> (cascading/load-tap tap '(fn [_ line] [line]))
                 (pigpen/store-tsv output1))]
    (-> cmd (cascading/generate-flow) (.complete))
    (is (= ["line1" "line2"] (read-output output1)))))

(deftest test-store-tap
  (write-input input1 ["line1" "line2"])
  (let [tap (Hfs. (TextLine. (Fields. (into-array (map str ["line"])))) output1)
        cmd (->> (pigpen/load-clj input1)
                 (cascading/store-tap tap))]
    (-> cmd (cascading/generate-flow) (.complete))
    (is (= ['line1 'line2] (read-output output1)))))

(deftest test-simple-flow
  (write-input input1 [["1" "2" "foo"] ["4" "5" "bar"]])
  (letfn
    [(func [data]
           (->> data
                (pigpen/map (fn [[a b c]]
                              {:sum  (+ (Integer/valueOf a) (Integer/valueOf b))
                               :name c}))
                (pigpen/filter (fn [{:keys [sum]}]
                                 (< sum 5)))))
     (query [input-file output-file]
            (->>
              (pigpen/load-clj input-file)
              (func)
              (pigpen/store-clj output-file)))]
    (let [flow (cascading/generate-flow (query input1 output1))]
      (.complete flow))
    (is (= [{:name "foo", :sum 3}] (read-output output1)))))

(deftest test-cogroup
  (write-input input1 [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
  (write-input input2 [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])
  (write-input input3 [{:c 1 :d "foo2"} {:c 2 :d "bar2"}])
  (let [left (pigpen/load-clj input1)
        middle (pigpen/load-clj input2)
        right (pigpen/load-clj input3)
        command (pigpen/cogroup [(left :on :a)
                                 (middle :on :c)
                                 (right :on :c)
                                 ]
                                (fn [k l m r] [k (map :b l) (map :d m) (map :d r)]))
        command (pigpen/store-clj output1 command)]
    (.complete (cascading/generate-flow command))
    (is (= '([1 (2 3) ("foo") ("foo2")] [2 (4) ("bar" "baz") ("bar2")]) (read-output output1)))))

(deftest test-inner-join
  (write-input input1 [{:a 1} {:a 2}])
  (write-input input2 [{:b 1} {:b 2}])
  (let [left (pigpen/load-clj input1)
        right (pigpen/load-clj input2)
        cmd (pigpen/join [(left :on :a)
                          (right :on :b)]
                         (fn [x y] [x y]))
        cmd (pigpen/store-clj output1 cmd)]
    (.complete (cascading/generate-flow cmd))
    (is (= '([{:a 1} {:b 1}] [{:a 2} {:b 2}]) (read-output output1)))))

(deftest test-outer-join
  (write-input input1 [{:a 1} {:a 2} {:a 3}])
  (write-input input2 [{:b 1} {:b 2} {:b 4}])
  (let [left (pigpen/load-clj input1)
        right (pigpen/load-clj input2)
        cmd (pigpen/join [(left :on :a :type :required)
                          (right :on :b :type :optional)]
                         (fn [x y] [x y]))
        cmd (pigpen/store-clj output1 cmd)]
    (.complete (cascading/generate-flow cmd))
    (is (= '([{:a 1} {:b 1}] [{:a 2} {:b 2}] [{:a 3} nil]) (read-output output1)))))

(deftest test-group-by
  (write-input input1 [{:a 1 :b 1} {:a 1 :b 2} {:a 2 :b 3}])
  (let [data (pigpen/load-clj input1)
        cmd (->> data
                 (pigpen/group-by :a)
                 (pigpen/store-clj output1))]
    (.complete (cascading/generate-flow cmd))
    (is (= '([1 ({:a 1, :b 1} {:a 1, :b 2})] [2 ({:a 2, :b 3})]) (read-output output1)))))

(deftest test-multiple-outputs
  (write-input input1 [1 2 3])
  (let [data (pigpen/load-clj input1)
        c1 (pigpen/map (fn [x] (* x 2)) data)
        c2 (pigpen/map (fn [x] (* x 3)) data)
        o1 (pigpen/store-clj output1 c1)
        o2 (pigpen/store-clj output2 c2)
        s (pigpen/store-many o1 o2)]
    (.complete (cascading/generate-flow s))
    (is (= '(2 4 6) (read-output output1)))
    (is (= '(3 6 9) (read-output output2)))))

(deftest test-distinct
  (write-input input1 [1 2 3])
  (let [data (pigpen/load-clj input1)
        cmd (->> data
                 (pigpen/mapcat (fn [x] [x (* x 2)]))
                 (pigpen/distinct)
                 (pigpen/store-clj output1))]
    (.complete (cascading/generate-flow cmd))
    (is (= #{1 2 4 3 6} (into #{} (read-output output1))))))

(deftest test-performance
  (let [in-fields (Fields. (into-array ["load1/value"]))
        out-fields (Fields. (into-array ["project1/value"]))
        context {:fields ['project1/value]
                 :projections [{:type :projection
                                :expr {:type :code
                                       :init nil
                                       :func (pigpen.runtime/exec [(pigpen.runtime/process->bind (pigpen.runtime/pre-process :cascading :frozen))
                                                                   (pigpen.runtime/map->bind identity)
                                                                   (pigpen.runtime/process->bind (pigpen.runtime/post-process :cascading :frozen))])
                                       :udf :seq
                                       :args ['load1/value]}
                                :flatten true
                                :alias ['project1/value]}]}
        value (->> 0
                runtime/cs-freeze
                vector
                into-array
                (Tuple.)
                (TupleEntry. in-fields))
        call (reify FunctionCall
               (getContext [this]
                 context)
               (setContext [this context]
                 (throw (ex-info "setContext" {})))
               (getArgumentFields [this]
                 (throw (ex-info "getArgumentFields" {})))
               (getArguments [this]
                 value)
               (getDeclaredFields [this]
                 (throw (ex-info "getDeclaredFields" {})))
               (getOutputCollector [this]
                 (proxy [TupleEntryCollector] [out-fields]
                   (collect [tuple]))))
        udf (PigPenFunction. nil out-fields)
        results (criterium/benchmark (.operate udf nil call) {})]
    (criterium/report-result results)
    (is (< (first (:mean results)) 1.6E-5))))
