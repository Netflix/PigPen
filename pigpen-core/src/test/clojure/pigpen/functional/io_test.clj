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

(ns pigpen.functional.io-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff]]
            [pigpen.core :as pig]
            [pigpen.fold :as fold]))

(.mkdirs (java.io.File. "build/functional/io-test"))

(deftest test-load-string
  (let [command (pig/load-string "build/functional/io-test/test-load-string")]
    (spit "build/functional/io-test/test-load-string" "The quick brown fox\njumps over the lazy dog\n")
    (test-diff
      (set (pig/dump command))
      '#{"The quick brown fox"
         "jumps over the lazy dog"})))  

(deftest test-load-tsv
  
  (testing "Normal tsv with default delimiter"
    (let [command (pig/load-tsv "build/functional/io-test/test-load-tsv")]
      (spit "build/functional/io-test/test-load-tsv" "a\tb\tc\n1\t2\t3\n")
      (test-diff
        (set (pig/dump command))
        '#{["a" "b" "c"]
           ["1" "2" "3"]})))
  
  (testing "Normal tsv with non-tab delimiter"
    (let [command (pig/load-tsv "build/functional/io-test/test-load-tsv" #",")]
      (spit "build/functional/io-test/test-load-tsv" "a\tb\tc\n1\t2\t3\n")
      (test-diff
        (set (pig/dump command))
        '#{["a\tb\tc"]
           ["1\t2\t3"]})))
  
  (testing "Non-tsv with non-tab delimiter"
    (let [command (pig/load-tsv "build/functional/io-test/test-load-tsv" #",")]
      (spit "build/functional/io-test/test-load-tsv" "a,b,c\n1,2,3\n")
      (test-diff
        (set (pig/dump command))
        '#{["a" "b" "c"]
           ["1" "2" "3"]}))))

(deftest test-load-clj
  (let [command (pig/load-clj "build/functional/io-test/test-load-clj")]
    (spit "build/functional/io-test/test-load-clj" "{:a 1, :b \"foo\"}\n{:a 2, :b \"bar\"}")
    (test-diff
      (set (pig/dump command))
      '#{{:a 2, :b "bar"}
         {:a 1, :b "foo"}})))

(deftest test-load-json
  
  (spit "build/functional/io-test/test-load-json" "{\"a\" 1, \"b\" \"foo\"}\n{\"a\" 2, \"b\" \"bar\"}")
  
  (testing "Default"
    (let [command (pig/load-json "build/functional/io-test/test-load-json")]
      (test-diff
        (set (pig/dump command))
        '#{{:a 2, :b "bar"}
           {:a 1, :b "foo"}})))
  
  (testing "No options"
    (let [command (pig/load-json "build/functional/io-test/test-load-json" {})]
      (test-diff
        (set (pig/dump command))
        '#{{"a" 2, "b" "bar"}
           {"a" 1, "b" "foo"}})))
  
  (testing "Two options"
    (let [command (pig/load-json "build/functional/io-test/test-load-json"
                                 {:key-fn keyword
                                  :value-fn (fn [k v]
                                              (case k
                                                :a (* v v)
                                                :b (count v)))})]
      (test-diff
        (set (pig/dump command))
        '#{{:a 1, :b 3}
           {:a 4, :b 3}}))))

(deftest test-load-lazy
  (let [command (pig/load-tsv "build/functional/io-test/test-load-lazy")]
    (spit "build/functional/io-test/test-load-lazy" "a\tb\tc\n1\t2\t3\n")
    (test-diff
      (set (pig/dump command))
      '#{("a" "b" "c")
         ("1" "2" "3")})))

(deftest test-store-string
  (let [data (pig/return ["The quick brown fox"
                          "jumps over the lazy dog"
                          42
                          :foo])
        command (pig/store-string "build/functional/io-test/test-store-string" data)]
    (is (= (pig/dump command)
           ["The quick brown fox" "jumps over the lazy dog" "42" ":foo"]))
    (is (= "The quick brown fox\njumps over the lazy dog\n42\n:foo\n"
           (slurp "build/functional/io-test/test-store-string")))))

(deftest test-store-tsv
  (let [data (pig/return [[1 "foo" :a]
                          [2 "bar" :b]])
        command (pig/store-tsv "build/functional/io-test/test-store-tsv" data)]
    (is (= (pig/dump command)
           ["1\tfoo\t:a" "2\tbar\t:b"]))
    (is (= "1\tfoo\t:a\n2\tbar\t:b\n"
           (slurp "build/functional/io-test/test-store-tsv")))))

(deftest test-store-clj
  (let [data (pig/return [(array-map :a 1, :b "foo")
                          (array-map :a 2, :b "bar")])
        command (pig/store-clj "build/functional/io-test/test-store-clj" data)]
    (is (= (pig/dump command)
           ["{:a 1, :b \"foo\"}" "{:a 2, :b \"bar\"}"]))
    (is (= "{:a 1, :b \"foo\"}\n{:a 2, :b \"bar\"}\n"
           (slurp "build/functional/io-test/test-store-clj")))))

(deftest test-store-json
  (let [data (pig/return [(array-map :a 1, :b "foo")
                          (array-map :a 2, :b "bar")])
        command (pig/store-json "build/functional/io-test/test-store-json" data)]
    (is (= (pig/dump command)
           ["{\"a\":1,\"b\":\"foo\"}" "{\"a\":2,\"b\":\"bar\"}"]))
    (is (= "{\"a\":1,\"b\":\"foo\"}\n{\"a\":2,\"b\":\"bar\"}\n"
           (slurp "build/functional/io-test/test-store-json")))))

(deftest test-return
  
  (let [data [1 2]
        command (pig/return data)]
    (test-diff
      (pig/dump command)
      '[1 2]))
  
  (let [command (->>
                  (pig/return [])
                  (pig/map inc))]
    (test-diff
      (pig/dump command)
      '[])))
