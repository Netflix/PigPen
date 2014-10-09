(ns pigpen.cascading-test
  (:import (java.io File)
           (org.apache.hadoop.fs FileSystem Path)
           (org.apache.hadoop.conf Configuration))
  (:use clojure.test pigpen.cascading)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.cascading.core :as pig]))

(def input1 "/tmp/input1")
(def input2 "/tmp/input2")
(def output "/tmp/output")

(defn setup [fn]
  (.delete (FileSystem/get (Configuration.)) (Path. output) true)
  (fn))

(clojure.test/use-fixtures :each setup)

(defn write-input [path objs]
  (spit path (clojure.string/join (map prn-str objs))))

(defn read-output [path]
  (map read-string (clojure.string/split (slurp (str path "/part-00000")) #"\n")))

(comment test-simple-flow
         (write-input input1 [["1" "2" "foo"] ["4" "5" "bar"]])
         (letfn
             [(func [data]
                    (->> data
                         (pig/map (fn [[a b c]]
                                    {:sum  (+ (Integer/valueOf a) (Integer/valueOf b))
                                     :name c}))
                         (pig/filter (fn [{:keys [sum]}]
                                       (< sum 5)))))
              (query [input-file output-file]
                     (->>
                       (load-clj input-file)
                       (func)
                       (store-clj output-file)))]
           (let [flow (generate-flow (query input1 output))]
             (.complete flow))
           (is (= [{:name "foo", :sum 3}] (read-output output)))))

(deftest test-cogroup
  (write-input input1 [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
  (write-input input2 [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])
  (let [left (load-clj input1)
        right (load-clj input2)
        command (pig/cogroup [(left :on :a)
                              (right :on :c)]
                             (fn [k l r] [k (map :b l) (map :d r)]))
        command (store-clj output command)]
    (.complete (generate-flow command))
    (is (= '([1 (2 3) ("foo")] [2 (4) ("bar" "baz")]) (read-output output)))))

(run-tests 'pigpen.cascading-test)
