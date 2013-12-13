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

(ns pigpen.README-test
  (:use clojure.test))

(.mkdirs (java.io.File. "build/readme-test"))
(require '[pigpen.core :as pig])

(defn word-count [lines]
  (->> lines
    (pig/mapcat #(-> % first
                   (clojure.string/lower-case)
                   (clojure.string/replace #"[^a-zA-Z0-9\s]" "")
                   (clojure.string/split #"\s+")))
    (pig/group-by identity)
    (pig/map (fn [[word occurrences]] [word (count occurrences)]))))

(defn word-count-query [input output]
  (->>
    (pig/load-tsv input)
    (word-count)
    (pig/store-tsv output)))

(deftest test-word-count
  (let [data (pig/return [["The fox jumped over the dog."]
                          ["The cow jumped over the moon."]])]
    (is (= (set (pig/dump (word-count data)))
           #{["moon" 1] ["jumped" 2] ["dog" 1] ["over" 2] ["cow" 1] ["fox" 1] ["the" 4]}))))

(defn reusable-fn [lower-bound data]
  (let [upper-bound (+ lower-bound 10)]
    (pig/filter (fn [x] (< lower-bound x upper-bound)) data)))

(deftest test-reusable-fn
  (let [command (->>
                  (pig/return [2 10 20])
                  (reusable-fn 5))]
    (is (= (pig/dump command)
           [10]))))

(deftest test-write-script
  (pig/write-script "build/readme-test/word-count.pig" (word-count-query "input.tsv" "output.tsv")))

(defn my-data-1 []
  (pig/load-tsv "build/readme-test/input.tsv"))

(spit "build/readme-test/input.tsv" "1\t2\tfoo\n4\t5\tbar")

(deftest test-mydata-1
  (is (= (pig/dump (my-data-1))
         [["1" "2" "foo"] ["4" "5" "bar"]])))

(defn my-data-2 []
  (->>
    (pig/load-tsv "build/readme-test/input.tsv")
    (pig/map (fn [[a b c]]
               {:sum (+ (Integer/valueOf a) (Integer/valueOf b))
                :name c}))))

(deftest test-mydata-2
  (is (= (pig/dump (my-data-2))
         [{:sum 3, :name "foo"} {:sum 9, :name "bar"}])))

(defn my-data-3 []
  (->>
    (pig/load-tsv "build/readme-test/input.tsv")
    (pig/map (fn [[a b c]]
               {:sum (+ (Integer/valueOf a) (Integer/valueOf b))
                :name c}))
    (pig/filter (fn [{:keys [sum]}]
                  (< sum 5)))))

(deftest test-mydata-3
  (is (= (pig/dump (my-data-3))
         [{:sum 3, :name "foo"}])))

(defn my-data [input-file]
  (pig/load-tsv input-file))

(defn my-func [data]
  (->> data
    (pig/map (fn [[a b c]]
               {:sum (+ a b)
                :name c}))
    (pig/filter (fn [{:keys [sum]}]
                  (< sum 5)))))

(defn my-query [input-file output-file]
  (->>
    (my-data input-file)
    (my-func)
    (pig/store-clj output-file)))

(deftest test-my-func
  (let [data (pig/return [[1 2 "foo"] [4 5 "bar"]])]
    (is (= (pig/dump (my-func data))
           [{:sum 3, :name "foo"}]))))

(deftest test-write-script2
  (pig/write-script "build/readme-test/my-script.pig" (my-query "build/readme-test/input.tsv" "build/readme-test/output.clj")))

(deftest test-join
  (let [left  (pig/return [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
        right (pig/return [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])

        command (pig/join (left on :a)
                          (right on :c)
                          (fn [l r] [(:b l) (:d r)]))]

    (is (= (pig/dump command)
           [[2 "foo"]
            [3 "foo"]
            [4 "bar"]
            [4 "baz"]]))))

(deftest test-cogroup
  (let [left  (pig/return [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
        right (pig/return [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])

        command (pig/cogroup (left on :a)
                             (right on :c)
                             (fn [k l r] [k (map :b l) (map :d r)]))]

    (is (= (pig/dump command)
           [[1 [2 3] ["foo"]]
            [2 [4]   ["bar" "baz"]]]))))

(spit "build/readme-test/numbers0.tsv" "1\t1\n2\t5")
(spit "build/readme-test/numbers1.tsv" "1\t2\n2\t6")
(spit "build/readme-test/numbers2.tsv" "1\t3\n2\t7")
(spit "build/readme-test/numbers3.tsv" "1\t4\n2\t8")

(deftest test-names

  (let [a (pig/load-pig "build/readme-test/numbers0.tsv" [i w])
        b (pig/load-pig "build/readme-test/numbers1.tsv" [i x])
        c (pig/load-pig "build/readme-test/numbers2.tsv" [i y])
        d (pig/load-pig "build/readme-test/numbers3.tsv" [i z])

        j0 (pig/join (a on :i)
                     (b on :i)
                     merge)
          
        j1 (pig/join (j0 on :i)
                     (c on :i)
                     merge)
          
        j2 (pig/join (j1 on :i)
                     (d on :i)
                     merge)]
    (is (= (pig/dump j2)
           [{:i 1, :w 1, :x 2, :y 3, :z 4}
            {:i 2, :w 5, :x 6, :y 7, :z 8}]))))





