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

(ns pigpen.functional.io-test
  (:require [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff]]
            [pigpen.io :as pig-io]
            [clojure.java.io :as io])
  (:import [java.util.zip GZIPOutputStream]))

(t/deftest test-load-string
  "normal load string"
  [harness]
  (let [file (t/write harness ["The quick brown fox" "jumps over the lazy dog"])]
    (test-diff
      (->>
        (pig-io/load-string file)
        (t/dump harness)
        (set))
      '#{"The quick brown fox"
         "jumps over the lazy dog"})))

(t/deftest test-load-tsv
  "normal load tsv"
  [harness]
  (let [file (t/write harness ["a\tb\tc" "1\t2\t3"])]
    (test-diff
      (->>
        (pig-io/load-tsv file)
        (t/dump harness)
        (set))
      '#{["a" "b" "c"]
         ["1" "2" "3"]})))

(t/deftest test-load-tsv-non-tab
  "load tsv, non-tab"
  [harness]
  (let [file (t/write harness ["a,b,c" "1,2,3"])]
    (test-diff
      (->>
        (pig-io/load-tsv file #",")
        (t/dump harness)
        (set))
      '#{["a" "b" "c"]
         ["1" "2" "3"]})))

(t/deftest test-load-tsv-non-tab-with-tabs
  "load tsv, non-tab, with tabs"
  [harness]
  (let [file (t/write harness ["a\tb\tc" "1\t2\t3"])]
    (test-diff
      (->>
        (pig-io/load-tsv file #",")
        (t/dump harness)
        (set))
      '#{["a\tb\tc"]
         ["1\t2\t3"]})))

(t/deftest test-load-csv-default-seperator-quotes
  "Normal csv with default separator and quotes"
  [harness]
  (let [file (t/write harness ["\"a string\",123,5.0" "\"a \"\"complex\"\" string\",-532,23.7"])]
    (test-diff
      (->>
        (pig-io/load-csv file)
        (t/dump harness)
        (set))
      '#{["a string" "123" "5.0"]
         ["a \"complex\" string" "-532" "23.7"]})))

(t/deftest test-load-csv-non-comma-seperator-different-quotes
  "Normal csv with non-comma separator and different quoting"
  [harness]
  (let [file (t/write harness ["\"a string\",123,5.0" "\"another string\",-532,23.7"])]
    (test-diff
      (->>
        (pig-io/load-csv file \; \')
        (t/dump harness)
        (set))
      '#{["\"a string\",123,5.0"]
         ["\"another string\",-532,23.7"]})))

(t/deftest test-load-csv-semicolon-delimiter-single-quotor
  "Non-csv with semicolon delimiter and single-quote quotor"
  [harness]
  (let [file (t/write harness ["'a string';123;5.0" "'another string';-532;23.7"])]
    (test-diff
      (->>
        (pig-io/load-csv file \; \')
        (t/dump harness)
        (set))
      '#{["a string" "123" "5.0"]
         ["another string" "-532" "23.7"]})))

(t/deftest test-load-clj
  "normal load clj"
  [harness]
  (let [file (t/write harness ["{:a 1, :b \"foo\"}" "{:a 2, :b \"bar\"}"])]
    (test-diff
      (->>
        (pig-io/load-clj file)
        (t/dump harness)
        (set))
      '#{{:a 2, :b "bar"}
         {:a 1, :b "foo"}})))

(t/deftest test-load-gz
  "gz input"
  [harness]
  (let [file (str (t/file harness) ".gz")]
    (with-open [o (GZIPOutputStream. (io/output-stream file))]
      (.write o (.getBytes "{:a 1, :b \"foo\"}\n{:a 2, :b \"bar\"}")))
    (test-diff
      (->>
        (pig-io/load-clj file)
        (t/dump harness)
        (set))
      '#{{:a 2, :b "bar"}
         {:a 1, :b "foo"}})))

(t/deftest test-load-json
  "normal load json"
  [harness]
  (let [file (t/write harness ["{\"a\" 1, \"b\" \"foo\"}" "{\"a\" 2, \"b\" \"bar\"}"])]
    (test-diff
      (->>
        (pig-io/load-json file)
        (t/dump harness)
        (set))
      '#{{:a 2, :b "bar"}
         {:a 1, :b "foo"}})))

(t/deftest test-load-json-no-options
  "load json, no options"
  [harness]
  (let [file (t/write harness ["{\"a\" 1, \"b\" \"foo\"}" "{\"a\" 2, \"b\" \"bar\"}"])]
    (test-diff
      (->>
        (pig-io/load-json file {})
        (t/dump harness)
        (set))
      '#{{"a" 2, "b" "bar"}
         {"a" 1, "b" "foo"}})))

(t/deftest test-load-json-two-options
  "load json, two options"
  [harness]
  (let [file (t/write harness ["{\"a\" 1, \"b\" \"foo\"}" "{\"a\" 2, \"b\" \"bar\"}"])]
    (test-diff
      (->>
        (pig-io/load-json file {:key-fn keyword
                             :value-fn (fn [k v]
                                         (case k
                                           :a (* v v)
                                           :b (count v)))})
        (t/dump harness)
        (set))
      '#{{:a 1, :b 3}
         {:a 4, :b 3}})))

(t/deftest test-load-lazy
  "lazy-seq loader"
  [harness]
  (let [file (t/write harness ["a\tb\tc" "1\t2\t3"])]
    (test-diff
      (->>
        (pig-io/load-tsv file)
        (t/dump harness)
        (set))
      '#{("a" "b" "c")
         ("1" "2" "3")})))

(t/deftest test-store-string
  "normal store string"
  [harness]
  (let [file (t/file harness)]
    (->>
      (t/data harness ["The quick brown fox"
                       "jumps over the lazy dog"
                       42
                       :foo])
      (pig-io/store-string file)
      (t/dump harness))
    (test-diff (t/read harness file)
               ["The quick brown fox" "jumps over the lazy dog" "42" ":foo"])))

(t/deftest test-store-tsv
  "normal store tsv"
  [harness]
  (let [file (t/file harness)]
    (->>
      (t/data harness [[1 "foo" :a]
                       [2 "bar" :b]])
      (pig-io/store-tsv file)
      (t/dump harness))
    (test-diff (t/read harness file)
               ["1\tfoo\t:a" "2\tbar\t:b"])))

(t/deftest test-store-clj
  "normal store clj"
  [harness]
  (let [file (t/file harness)]
    (->>
      (t/data harness [(array-map :a 1, :b "foo")
                       (array-map :a 2, :b "bar")])
      (pig-io/store-clj file)
      (t/dump harness))
    (test-diff (t/read harness file)
               ["{:a 1, :b \"foo\"}" "{:a 2, :b \"bar\"}"])))

(t/deftest test-store-json
  "normal store json"
  [harness]
  (let [file (t/file harness)]
    (->>
      (t/data harness [(array-map :a 1, :b "foo")
                       (array-map :a 2, :b "bar")])
      (pig-io/store-json file)
      (t/dump harness))
    (test-diff (t/read harness file)
               ["{\"a\":1,\"b\":\"foo\"}" "{\"a\":2,\"b\":\"bar\"}"])))
