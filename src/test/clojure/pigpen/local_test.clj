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

(ns pigpen.local-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.local :as local]
            [pigpen.pig :refer [freeze-vals thaw-anything]]
            [pigpen.raw :as raw]
            [pigpen.io :as io]
            [pigpen.map :as pig-map]
            [pigpen.filter :as pig-filter]
            [pigpen.set :as pig-set]
            [pigpen.join :as pig-join]
            [pigpen.exec :as exec]
            [pigpen.fold :as fold]
            [taoensso.nippy :refer [freeze thaw]])
  (:import [rx Observable]
           [rx.observables BlockingObservable]
           [org.apache.pig.data DataByteArray DataBag]))

(.mkdirs (java.io.File. "build/local-test"))

(deftest test-eval-code
  
  (let [code (raw/code$ DataBag '[x y]
               (raw/expr$ '(require (quote [pigpen.pig]))
                          '(pigpen.pig/exec
                             [(pigpen.pig/pre-process :frozen)
                              (pigpen.pig/map->bind (fn [x y] (+ x y)))
                              (pigpen.pig/post-process :frozen)])))
        values (freeze-vals {'x 37, 'y 42})]
    (is (= (thaw-anything (#'pigpen.local/eval-code code values))
           '(bag (tuple (freeze 79)))))))

(deftest test-cross-product
  
  (testing "normal join"
    (let [data '[[{r1v1 1, r1v2 2} {r1v1 3, r1v2 4}]
                 [{r2v1 5, r2v2 6} {r2v1 7, r2v2 8}]]]
      (test-diff
        (set (#'pigpen.local/cross-product data))
        '#{{r1v1 1, r1v2 2, r2v2 6, r2v1 5}
           {r1v1 1, r1v2 2, r2v2 8, r2v1 7}
           {r1v1 3, r1v2 4, r2v2 6, r2v1 5}
           {r1v1 3, r1v2 4, r2v2 8, r2v1 7}})))
  
  (testing "single flatten"
    (let [data '[[{key 1}]
                 [{val1 :a}]
                 [{val2 "a"}]]]
      (test-diff
        (set (#'pigpen.local/cross-product data))
        '#{{key 1, val1 :a, val2 "a"}})))
  
  (testing "multi flatten"
    (let [data '[[{key 1}]
                 [{val1 :a} {val1 :b}]
                 [{val2 "a"} {val2 "b"}]]]
      (test-diff
        (set (#'pigpen.local/cross-product data))
        '#{{key 1, val1 :a, val2 "a"}
           {key 1, val1 :a, val2 "b"}
           {key 1, val1 :b, val2 "a"}
           {key 1, val1 :b, val2 "b"}})))
  
  (testing "inner join"
    (let [data '[[{r1v1 1, r1v2 2} {r1v1 3, r1v2 4}]
                 [{r2v1 5, r2v2 6} {r2v1 7, r2v2 8}]
                 []]]
      (test-diff
        (set (#'pigpen.local/cross-product data))
        '#{})))
  
  (testing "outer join"
    (let [data '[[{r1v1 1, r1v2 2} {r1v1 3, r1v2 4}]
                 [{r2v1 5, r2v2 6} {r2v1 7, r2v2 8}]
                 [{}]]]
      (test-diff
        (set (#'pigpen.local/cross-product data))
        '#{{r1v1 1, r1v2 2, r2v2 6, r2v1 5}
           {r1v1 1, r1v2 2, r2v2 8, r2v1 7}
           {r1v1 3, r1v2 4, r2v2 6, r2v1 5}
           {r1v1 3, r1v2 4, r2v2 8, r2v1 7}}))))

(deftest test-debug
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [command (->>
                    (io/load-clj "build/local-test/test-debug-in")
                    (pig-filter/filter (comp odd? :a))
                    (pig-map/map :b)
                    (io/store-clj "build/local-test/test-debug-out"))]
      (spit "build/local-test/test-debug-in" "{:a 1, :b \"foo\"}\n{:a 2, :b \"bar\"}")
      #_(exec/write-script "build/local-test/temp.pig" {:debug "build/local-test/"} command)
      (is (empty? (exec/dump-debug "build/local-test/test-debug-" command)))
      (is (= "class java.lang.String\t{:a 1, :b \"foo\"}\nclass java.lang.String\t{:a 2, :b \"bar\"}\n"
            (slurp "build/local-test/test-debug-load1")))
      (is (= "class clojure.lang.PersistentArrayMap\t{:a 1, :b \"foo\"}\nclass clojure.lang.PersistentArrayMap\t{:a 2, :b \"bar\"}\n"
            (slurp "build/local-test/test-debug-bind2")))
      (is (= "class clojure.lang.PersistentArrayMap\t{:a 1, :b \"foo\"}\n"
            (slurp "build/local-test/test-debug-bind3")))
      (is (= "class java.lang.String\tfoo\n"
            (slurp "build/local-test/test-debug-bind4")))
      (is (= "class java.lang.String\t\"foo\"\n"
            (slurp "build/local-test/test-debug-bind5"))))))

;; ********** IO **********

(deftest test-load
  (let [command (raw/load$ "build/local-test/test-load" '[a b c] raw/default-storage {:cast "chararray"})]
    (spit "build/local-test/test-load" "a\tb\tc\n1\t2\t3\n")
    (test-diff
      (exec/debug-script-raw command)
      '[{a "a", b "b", c "c"}
        {a "1", b "2", c "3"}])))

(deftest test-load-pig
  (let [command (io/load-pig "build/local-test/test-load-pig" [a b])]
    (spit "build/local-test/test-load-pig" "1\tfoo\n2\tbar\n")
    (test-diff
      (set (exec/debug-script command))
      '#{(freeze {:a 2, :b "bar"})
         (freeze {:a 1, :b "foo"})})))  

(deftest test-load-string
  (let [command (io/load-string "build/local-test/test-load-string")]
    (spit "build/local-test/test-load-string" "The quick brown fox\njumps over the lazy dog\n")
    (test-diff
      (set (exec/debug-script command))
      '#{(freeze "The quick brown fox")
         (freeze "jumps over the lazy dog")})))  

(deftest test-load-tsv
  
  (testing "Normal tsv with default delimiter"
    (let [command (io/load-tsv "build/local-test/test-load-tsv")]
      (spit "build/local-test/test-load-tsv" "a\tb\tc\n1\t2\t3\n")
      (test-diff
        (set (exec/debug-script command))
        '#{(freeze ["a" "b" "c"])
           (freeze ["1" "2" "3"])})))
  
  (testing "Normal tsv with non-tab delimiter"
    (let [command (io/load-tsv "build/local-test/test-load-tsv" #",")]
      (spit "build/local-test/test-load-tsv" "a\tb\tc\n1\t2\t3\n")
      (test-diff
        (set (exec/debug-script command))
        '#{(freeze ["a\tb\tc"])
           (freeze ["1\t2\t3"])})))
  
  (testing "Non-tsv with non-tab delimiter"
    (let [command (io/load-tsv "build/local-test/test-load-tsv" #",")]
      (spit "build/local-test/test-load-tsv" "a,b,c\n1,2,3\n")
      (test-diff
        (set (exec/debug-script command))
        '#{(freeze ["a" "b" "c"])
           (freeze ["1" "2" "3"])}))))

(deftest test-load-clj
  (let [command (io/load-clj "build/local-test/test-load-clj")]
    (spit "build/local-test/test-load-clj" "{:a 1, :b \"foo\"}\n{:a 2, :b \"bar\"}")
    (test-diff
      (set (exec/debug-script command))
      '#{(freeze {:a 2, :b "bar"})
         (freeze {:a 1, :b "foo"})})))

(deftest test-load-json
  
  (spit "build/local-test/test-load-json" "{\"a\" 1, \"b\" \"foo\"}\n{\"a\" 2, \"b\" \"bar\"}")
  
  (testing "Default"
    (let [command (io/load-json "build/local-test/test-load-json")]
      (test-diff
        (set (exec/debug-script command))
        '#{(freeze {:a 2, :b "bar"})
           (freeze {:a 1, :b "foo"})})))
  
  (testing "No options"
    (let [command (io/load-json "build/local-test/test-load-json" {})]
      (test-diff
        (set (exec/debug-script command))
        '#{(freeze {"a" 2, "b" "bar"})
           (freeze {"a" 1, "b" "foo"})})))
  
  (testing "Two options"
    (let [command (io/load-json "build/local-test/test-load-json"
                                {:key-fn keyword
                                 :value-fn (fn [k v]
                                             (case k
                                               :a (* v v)
                                               :b (count v)))})]
      (test-diff
        (set (exec/debug-script command))
        '#{(freeze {:a 1, :b 3})
           (freeze {:a 4, :b 3})}))))

(deftest test-load-lazy
  (let [command (io/load-tsv "build/local-test/test-load-lazy")]
    (spit "build/local-test/test-load-lazy" "a\tb\tc\n1\t2\t3\n")
    (test-diff
      (set (exec/debug-script command))
      '#{(freeze ("a" "b" "c"))
         (freeze ("1" "2" "3"))})))

(deftest test-store
  (let [data (io/return-raw '[{a "a", b "b", c "c"}
                              {a "1", b "2", c "3"}])
        command (raw/store$ data "build/local-test/test-store" raw/default-storage {})]
    (is (empty? (exec/debug-script command)))
    (is (= "a\tb\tc\n1\t2\t3\n"
           (slurp "build/local-test/test-store")))))

(deftest test-store-pig
  (let [data (io/return [{:a 1, :b "foo"}
                         {:a 2, :b "bar"}])
        command (io/store-pig "build/local-test/test-store-pig" data)]
    (is (empty? (exec/debug-script command)))
    (is (= "[a#1,b#foo]\n[a#2,b#bar]\n"
           (slurp "build/local-test/test-store-pig")))))

(deftest test-store-string
  (let [data (io/return ["The quick brown fox"
                         "jumps over the lazy dog"
                         42
                         :foo])
        command (io/store-string "build/local-test/test-store-string" data)]
    (is (empty? (exec/debug-script command)))
    (is (= "The quick brown fox\njumps over the lazy dog\n42\n:foo\n"
           (slurp "build/local-test/test-store-string")))))

(deftest test-store-tsv
  (let [data (io/return [[1 "foo" :a]
                         [2 "bar" :b]])
        command (io/store-tsv "build/local-test/test-store-tsv" data)]
    (is (empty? (exec/debug-script command)))
    (is (= "1\tfoo\t:a\n2\tbar\t:b\n"
           (slurp "build/local-test/test-store-tsv")))))

(deftest test-store-clj
  (let [data (io/return [{:a 1, :b "foo"}
                         {:a 2, :b "bar"}])
        command (io/store-clj "build/local-test/test-store-clj" data)]
    (is (empty? (exec/debug-script command)))
    (is (= "{:a 1, :b \"foo\"}\n{:a 2, :b \"bar\"}\n"
           (slurp "build/local-test/test-store-clj")))))

(deftest test-store-json
  (let [data (io/return [{:a 1, :b "foo"}
                         {:a 2, :b "bar"}])
        command (io/store-json "build/local-test/test-store-json" data)]
    (is (empty? (exec/debug-script command)))
    (is (= "{\"a\":1,\"b\":\"foo\"}\n{\"a\":2,\"b\":\"bar\"}\n"
           (slurp "build/local-test/test-store-json")))))

(deftest test-return
  
  (let [data [1 2]
        command (io/return data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 1) (freeze 2)])))

;; ********** Map **********

(deftest test-map
  
  (let [data (io/return [{:x 1, :y 2}
                         {:x 2, :y 4}])
        command (pig-map/map (fn [{:keys [x y]}] (+ x y)) data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 3) (freeze 6)]))
  
  (let [data (io/return [1 2 3])
        command (pig-map/map (fn [x] (throw (java.lang.Exception.))) data)]
    (is (thrown? Exception (exec/debug-script command)))))

(defn test-fn [x]
  (* x x))

(defn test-param [y data]
  (let [z 42]
    (->> data
      (pig-map/map (fn [x] (+ (test-fn x) y z))))))

(deftest test-closure
  (let [data (io/return [1 2 3])
        command (test-param 37 data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 80) (freeze 83) (freeze 88)])))

(deftest test-mapcat
  
  (let [data (io/return [{:x 1, :y 2}
                         {:x 2, :y 4}])
        command (pig-map/mapcat (juxt :x :y) data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 1)
        (freeze 2)
        (freeze 2)
        (freeze 4)])))

(deftest test-map-indexed
  
  (let [data (io/return [{:a 2} {:a 1} {:a 3}])
        command (pig-map/map-indexed vector data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze [0 {:a 2}]) (freeze [1 {:a 1}]) (freeze [2 {:a 3}])]))
  
  (let [command (->>
                  (io/return [{:a 2} {:a 1} {:a 3}])
                  (pig-map/sort-by :a)
                  (pig-map/map-indexed vector))]
    (test-diff
      (exec/debug-script command)
      '[(freeze [0 {:a 1}]) (freeze [1 {:a 2}]) (freeze [2 {:a 3}])])))

(deftest test-pig-compare
  (is (= -1 (#'pigpen.local/pig-compare ['key :asc] '{key 1} '{key 2})))
  (is (= 1 (#'pigpen.local/pig-compare ['key :desc] '{key 1} '{key 2})))
  (is (= -1 (#'pigpen.local/pig-compare ['key :desc 'value :asc] '{key 1 value 1} '{key 1 value 2}))))

(deftest test-sort
  
  (let [data (io/return [2 1 4 3])
        command (pig-map/sort data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 1) (freeze 2) (freeze 3) (freeze 4)]))
  
  (let [data (io/return [2 1 4 3])
        command (pig-map/sort :desc data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 4) (freeze 3) (freeze 2) (freeze 1)])))

(deftest test-sort-by
  
  (let [data (io/return [{:a 2} {:a 1} {:a 3}])
        command (pig-map/sort-by :a data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze {:a 1}) (freeze {:a 2}) (freeze {:a 3})]))
  
  (let [data (io/return [{:a 2} {:a 1} {:a 3}])
       command (pig-map/sort-by :a :desc data)]
   (test-diff
     (exec/debug-script command)
     '[(freeze {:a 3}) (freeze {:a 2}) (freeze {:a 1})]))
  
  (let [data (io/return [1 2 3 1 2 3 1 2 3])
        command (pig-map/sort-by identity data)]
    (is (= (exec/dump command)
           [1 1 1 2 2 2 3 3 3]))))

(deftest test-for
  (is (= (pigpen.exec/dump
           (apply pig-set/concat
             (for [x [1 2 3]]
               (->>
                 (io/return [1 2 3])
                 (pig-map/map (fn [y] (+ x y)))))))
         [4 3 2 5 4 3 6 5 4])))

(deftest test-map+fold
  (is (= (pigpen.exec/dump
           (->> (io/return [-2 -1 0 1 2])
             (pig-map/map #(> % 0))
             (pig-join/fold (->> (fold/filter identity) (fold/count)))))
         [2])))

(deftest test-map+reduce
  (is (= (pigpen.exec/dump
           (->> (io/return [-2 -1 0 1 2])
             (pig-map/map inc)
             (pig-join/fold (->> (fold/filter #(> % 0)) (fold/count)))))
         [3])))

;; ********** Filter **********

(deftest test-filter
  
  (let [data (io/return [1 2])
        command (pig-filter/filter odd? data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 1)])))

(deftest test-filter-native
  
  (let [data (io/return-raw '[{foo 1, bar 3}
                              {foo 2, bar 1}])
        command (raw/filter-native$ data '(and (= foo 1) (> bar 2)) {:fields '[foo bar]})
        command' (raw/filter-native$ data nil {:fields '[foo bar]})]
    (test-diff
      (exec/debug-script-raw command)
      '[{foo 1, bar 3}])
    (test-diff
      (exec/debug-script-raw command')
      '[{foo 1, bar 3}
        {foo 2, bar 1}])))

(deftest test-remove
  
  (let [data (io/return [1 2])
        command (pig-filter/remove odd? data)]
    (test-diff
      (exec/debug-script command)
      '[(freeze 2)])))

(deftest test-limit
  
  (let [data (io/return-raw
               (map freeze-vals '[{x 1, y 2}
                                  {x 2, y 4}]))
        command (raw/limit$ data 1 {})]
    (test-diff
      (exec/debug-script-raw command)
      '[{x (freeze 1), y (freeze 2)}])))

(deftest test-sample
  
  (let [data (io/return (repeat 1000 {:x 1, :y 2}))
        command (raw/sample$ data 0.5 {})]
    (< 490
       (count (exec/debug-script command))
       510)))

;; ********** Set **********

(deftest test-distinct
  
  (let [data (io/return [{:x 1, :y 2}
                         {:x 2, :y 4}
                         {:x 1, :y 2}
                         {:x 1, :y 2}
                         {:x 2, :y 4}])
        command (raw/distinct$ data {})]
    (test-diff
      (exec/debug-script command)
      '[(freeze {:y 2, :x 1})
        (freeze {:y 4, :x 2})])))

(deftest test-union
  (let [data1 (io/return [1 2 3])
        data2 (io/return [2 3 4])
        data3 (io/return [3 4 5])
        
        command (pig-set/union data1 data2 data3)]
    (test-diff
      (set (exec/debug-script command))
      '#{(freeze 1) (freeze 2) (freeze 3) (freeze 4) (freeze 5)})))

(deftest test-union-multiset
  (let [data1 (io/return [1 2 3])
        data2 (io/return [2 3 4])
        data3 (io/return [3 4 5])
        
        command (pig-set/union-multiset data1 data2 data3)]
    (test-diff
      (sort-by second (exec/debug-script command))
      '[(freeze 1) (freeze 2) (freeze 2)
        (freeze 3) (freeze 3) (freeze 3)
        (freeze 4) (freeze 4) (freeze 5)])))

(deftest test-intersection
  (let [data1 (io/return [1 2 3 3])
        data2 (io/return [3 2 3 4 3])
        data3 (io/return [3 4 3 5 2])
        
        command (pig-set/intersection data1 data2 data3)]
    (test-diff
      (sort-by second (exec/debug-script command))
      '[(freeze 2) (freeze 3)])))

(deftest test-intersection-multiset
  (let [data1 (io/return [1 2 3 3])
        data2 (io/return [3 2 3 4 3])
        data3 (io/return [3 4 3 5 2])
        
        command (pig-set/intersection-multiset data1 data2 data3)]
    (test-diff
      (sort-by second (exec/debug-script command))
      '[(freeze 2) (freeze 3) (freeze 3)])))

(deftest test-difference
  (let [data1 (io/return [1 2 3 3 3 4 5])
        data2 (io/return [1 2])
        data3 (io/return [4 5])
        
        command (pig-set/difference data1 data2 data3)]
    (test-diff
      (sort-by second (exec/debug-script command))
      '[(freeze 3)])))

(deftest test-difference-multiset
  (let [data1 (io/return [1 2 3 3 3 4 5])
        data2 (io/return [1 2 3])
        data3 (io/return [3 4 5])
        
        command (pig-set/difference-multiset data1 data2 data3)]
    (test-diff
      (sort-by second (exec/debug-script command))
      '[(freeze 3)])))

;; ********** Join **********

(deftest test-group-by
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [data (io/return [{:a 1 :b 2}
                           {:a 1 :b 3}
                           {:a 2 :b 4}])
          
          command (pig-join/group-by :a data)]

      (test-diff
        (set (exec/debug-script command))
        '#{(freeze [1 ({:a 1, :b 2} {:a 1, :b 3})])
           (freeze [2 ({:a 2, :b 4})])}))))

(deftest test-into
  (let [data (io/return-raw
               (map freeze-vals '[{value 2}
                                  {value 4}
                                  {value 6}]))
        
        command (pig-join/into [] data)]

    (test-diff
      (exec/debug-script command)
      '[(freeze [2 4 6])])))

(deftest test-reduce
  (let [data (io/return-raw
               (map freeze-vals '[{value 2}
                                  {value 4}
                                  {value 6}]))
        
        command (pig-join/reduce conj [] data)]

    (test-diff
      (exec/debug-script command)
      '[(freeze [2 4 6])]))
  
  (let [data (io/return-raw
               (map freeze-vals '[{value 2}
                                  {value 4}
                                  {value 6}]))
        
        command (pig-join/reduce + data)]

    (test-diff
      (exec/debug-script command)
      '[(freeze 12)])))

(deftest test-fold
  (let [data (io/return [{:k :foo, :v 1}
                         {:k :foo, :v 2}
                         {:k :foo, :v 3}
                         {:k :bar, :v 4}
                         {:k :bar, :v 5}])]
    (let [command (->> data
                    (pig-join/group-by :k
                                       {:fold (fold/fold-fn + (fn [acc value] (+ acc (:v value))))}))]
      (is (= (set (exec/debug-script command))
             '#{(freeze [:foo 6])
                (freeze [:bar 9])})))
    
    (let [command (->> data
                    (pig-join/group-by :k
                                       {:fold (fold/fold-fn (fn ([] 0)
                                                              ([a b] (+ a b)))
                                                                (fn [acc _] (inc acc)))}))]
      (is (= (set (exec/debug-script command))
             '#{(freeze [:bar 2])
                (freeze [:foo 3])})))
    
    (let [command (->> data
                    (pig-join/group-by :k
                                       {:fold (fold/count)}))]
      (is (= (set (exec/debug-script command))
             '#{(freeze [:bar 2])
                (freeze [:foo 3])}))))
  
  (let [data0 (io/return [{:k :foo, :a 1}
                          {:k :foo, :a 2}
                          {:k :foo, :a 3}
                          {:k :bar, :a 4}
                          {:k :bar, :a 5}])
        data1 (io/return [{:k :foo, :b 1}
                          {:k :foo, :b 2}
                          {:k :bar, :b 3}
                          {:k :bar, :b 4}
                          {:k :bar, :b 5}])
        command (pig-join/cogroup [(data0 :on :k, :required true, :fold (->> (fold/map :a) (fold/sum)))
                                   (data1 :on :k, :required true, :fold (->> (fold/map :b) (fold/sum)))]
                                  vector)]
    (is (= (set (exec/debug-script command))
           '#{(freeze [:foo 6 3])
              (freeze [:bar 9 12])})))
  
  (let [data (io/return [1 2 3 4])
        command (pig-join/fold + data)]
    (is (= (exec/debug-script command)
           '[(freeze 10)])))
  
  (let [data (io/return [1 2 3 4])
        command (pig-join/fold (fold/count) data)]
   (is (= (exec/debug-script command)
          '[(freeze 4)]))))

(deftest test-cogroup
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [data1 (io/return-raw
                  (map freeze-vals '[{key :a, value 2}
                                     {key :a, value 4}
                                     {key :b, value 6}]))
          data2 (io/return-raw
                  (map freeze-vals '[{key :a, value 8}
                                     {key :b, value 10}
                                     {key :b, value 12}]))
          
          command (raw/group$ [data1 data2]
                              '[[key] [key]]
                              [:optional :optional]
                              {})]

      (test-diff
        (set (exec/debug-script-raw command))
        '#{{group (freeze :a)
            [[return1] key] (bag (tuple (freeze :a)) (tuple (freeze :a)))
            [[return1] value] (bag (tuple (freeze 2)) (tuple (freeze 4)))
            [[return2] key] (bag (tuple (freeze :a)))
            [[return2] value] (bag (tuple (freeze 8)))}
           {group (freeze :b)
            [[return1] key] (bag (tuple (freeze :b)))
            [[return1] value] (bag (tuple (freeze 6)))
            [[return2] key] (bag (tuple (freeze :b)) (tuple (freeze :b)))
            [[return2] value] (bag (tuple (freeze 10)) (tuple (freeze 12)))}}))))

(deftest test-cogroup+
  (let [data1 (io/return [{:a 1, :b 2}
                          {:a 1, :b 4}
                          {:a 2, :b 6}])
        data2 (io/return [{:a 1, :b 8}
                          {:a 2, :b 10}
                          {:a 2, :b 12}])
        
        command (pig-join/cogroup [(data1 :on :a) (data2 :on :a)] vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [1 ({:a 1, :b 2} {:a 1, :b 4}) ({:a 1, :b 8})])
         (freeze [2 ({:a 2, :b 6}) ({:a 2, :b 10} {:a 2, :b 12})])})))

(deftest test-cogroup-inner
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :by :k :type :required)
                                   (data2 :by :k :type :required)]
                                  vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])})))

(deftest test-cogroup-left-outer
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :on :k :type :required)
                                   (data2 :on :k :type :optional)]
                                  vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil
                  [{:k nil, :v 1} {:k nil, :v 3}]
                  nil])
         (freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])
         (freeze [:l
                  [{:k :l, :v 9} {:k :l, :v 11}]
                  nil])})))

(deftest test-cogroup-right-outer
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :on :k :type :optional)
                                   (data2 :on :k :type :required)]
                                  vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil
                  nil
                  [{:k nil, :v 2} {:k nil, :v 4}]])
         (freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])
         (freeze [:r
                  nil
                  [{:k :r, :v 10} {:k :r, :v 12}]])})))

(deftest test-cogroup-full-outer
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :on :k :type :optional)
                                   (data2 :on :k :type :optional)]
                                  vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil
                  [{:k nil, :v 1} {:k nil, :v 3}]
                  nil])
         (freeze [nil
                  nil
                  [{:k nil, :v 2} {:k nil, :v 4}]])
         (freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])
         (freeze [:l
                  [{:k :l, :v 9} {:k :l, :v 11}]
                  nil])
         (freeze [:r
                  nil
                  [{:k :r, :v 10} {:k :r, :v 12}]])})))

(deftest test-cogroup-inner-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :on :k :type :required)
                                   (data2 :on :k :type :required)]
                                  vector
                                  {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil
                  [{:k nil, :v 1} {:k nil, :v 3}]
                  [{:k nil, :v 2} {:k nil, :v 4}]])
         (freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])})))

(deftest test-cogroup-left-outer-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :on :k :type :required)
                                   (data2 :on :k :type :optional)]
                                  vector
                                  {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil
                  [{:k nil, :v 1} {:k nil, :v 3}]
                  [{:k nil, :v 2} {:k nil, :v 4}]])
         (freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])
         (freeze [:l
                  [{:k :l, :v 9} {:k :l, :v 11}]
                  nil])})))

(deftest test-cogroup-right-outer-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :on :k :type :optional)
                                   (data2 :on :k :type :required)]
                                  vector
                                  {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil
                  [{:k nil, :v 1} {:k nil, :v 3}]
                  [{:k nil, :v 2} {:k nil, :v 4}]])
         (freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])
         (freeze [:r
                  nil
                  [{:k :r, :v 10} {:k :r, :v 12}]])})))

(deftest test-cogroup-full-outer-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/cogroup [(data1 :on :k :type :optional)
                                   (data2 :on :k :type :optional)]
                                  vector
                                  {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil
                  [{:k nil, :v 1} {:k nil, :v 3}]
                  [{:k nil, :v 2} {:k nil, :v 4}]])
         (freeze [:i
                  [{:k :i, :v 5} {:k :i, :v 7}]
                  [{:k :i, :v 6} {:k :i, :v 8}]])
         (freeze [:l
                  [{:k :l, :v 9} {:k :l, :v 11}]
                  nil])
         (freeze [:r
                  nil
                  [{:k :r, :v 10} {:k :r, :v 12}]])})))

(deftest test-join
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [data1 (io/return-raw
                  (map freeze-vals '[{key :a, value 2}
                                     {key :a, value 4}
                                     {key :b, value 6}]))
          data2 (io/return-raw
                  (map freeze-vals '[{key :a, value 8}
                                     {key :b, value 10}
                                     {key :b, value 12}]))
          
          command (raw/join$ [data1 data2]
                             '[[key] [key]]
                             '[:required :required]
                             {})]
      
      (test-diff
        (set (exec/debug-script-raw command))
        #{'{[[return1 key]] (freeze :a), [[return1 value]] (freeze 2), [[return2 key]] (freeze :a), [[return2 value]] (freeze 8)}
          '{[[return1 key]] (freeze :a), [[return1 value]] (freeze 4), [[return2 key]] (freeze :a), [[return2 value]] (freeze 8)}
          '{[[return1 key]] (freeze :b), [[return1 value]] (freeze 6), [[return2 key]] (freeze :b), [[return2 value]] (freeze 10)}
          '{[[return1 key]] (freeze :b), [[return1 value]] (freeze 6), [[return2 key]] (freeze :b), [[return2 value]] (freeze 12)}}))))

(deftest test-join+
  (let [data1 (io/return [{:a 1, :b 2}
                          {:a 1, :b 4}
                          {:a 2, :b 6}])
        data2 (io/return [{:a 1, :b 8}
                          {:a 2, :b 10}
                          {:a 2, :b 12}])
        
        command (pig-join/join [(data1 :on :a) (data2 :on :a)] vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:a 1, :b 2} {:a 1, :b 8}])
         (freeze [{:a 1, :b 4} {:a 1, :b 8}])
         (freeze [{:a 2, :b 6} {:a 2, :b 10}])
         (freeze [{:a 2, :b 6} {:a 2, :b 12}])})))

(deftest test-join-identity
  (let [data1 (io/return [1 2])
        data2 (io/return [2 3])
        
        command (pig-join/join [(data1)
                                (data2)]
                               vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [2 2])})))

(deftest test-join-inner-implicit
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k)
                                (data2 :on :k)]
                               vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])})))

(deftest test-join-inner
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :required)
                                (data2 :on :k :type :required)]
                               vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])})))

(deftest test-join-left-outer
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :required)
                                (data2 :on :k :type :optional)]
                               vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k nil, :v 1} nil])
         (freeze [{:k nil, :v 3} nil])
         (freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])
         (freeze [{:k :l, :v 9} nil])
         (freeze [{:k :l, :v 11} nil])})))

(deftest test-join-right-outer
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :optional)
                                (data2 :on :k :type :required)]
                               vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [nil {:k nil, :v 2}])
         (freeze [nil {:k nil, :v 4}])
         (freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])
         (freeze [nil {:k :r, :v 10}])
         (freeze [nil {:k :r, :v 12}])})))

(deftest test-join-full-outer
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :optional)
                                (data2 :on :k :type :optional)]
                               vector)]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k nil, :v 1} nil])
         (freeze [{:k nil, :v 3} nil])
         (freeze [nil {:k nil, :v 2}])
         (freeze [nil {:k nil, :v 4}])
         (freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])
         (freeze [{:k :l, :v 9} nil])
         (freeze [{:k :l, :v 11} nil])
         (freeze [nil {:k :r, :v 10}])
         (freeze [nil {:k :r, :v 12}])})))

(deftest test-join-inner-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :required)
                                (data2 :on :k :type :required)]
                               vector
                               {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k nil, :v 1} {:k nil, :v 2}])
         (freeze [{:k nil, :v 3} {:k nil, :v 2}])
         (freeze [{:k nil, :v 1} {:k nil, :v 4}])
         (freeze [{:k nil, :v 3} {:k nil, :v 4}])
         (freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])})))

(deftest test-join-left-outer-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :required)
                                (data2 :on :k :type :optional)]
                               vector
                               {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k nil, :v 1} {:k nil, :v 2}])
         (freeze [{:k nil, :v 3} {:k nil, :v 2}])
         (freeze [{:k nil, :v 1} {:k nil, :v 4}])
         (freeze [{:k nil, :v 3} {:k nil, :v 4}])
         (freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])
         (freeze [{:k :l, :v 9} nil])
         (freeze [{:k :l, :v 11} nil])})))

(deftest test-join-right-outer-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :optional)
                                (data2 :on :k :type :required)]
                               vector
                               {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k nil, :v 1} {:k nil, :v 2}])
         (freeze [{:k nil, :v 3} {:k nil, :v 2}])
         (freeze [{:k nil, :v 1} {:k nil, :v 4}])
         (freeze [{:k nil, :v 3} {:k nil, :v 4}])
         (freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])
         (freeze [nil {:k :r, :v 10}])
         (freeze [nil {:k :r, :v 12}])})))

(deftest test-join-full-outer-join-nils
  (let [data1 (io/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (io/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])
        
        command (pig-join/join [(data1 :on :k :type :optional)
                                (data2 :on :k :type :optional)]
                               vector
                               {:join-nils true})]

    (test-diff
      (set (exec/debug-script command))
      '#{(freeze [{:k nil, :v 1} {:k nil, :v 2}])
         (freeze [{:k nil, :v 3} {:k nil, :v 2}])
         (freeze [{:k nil, :v 1} {:k nil, :v 4}])
         (freeze [{:k nil, :v 3} {:k nil, :v 4}])
         (freeze [{:k :i, :v 5} {:k :i, :v 6}])
         (freeze [{:k :i, :v 5} {:k :i, :v 8}])
         (freeze [{:k :i, :v 7} {:k :i, :v 6}])
         (freeze [{:k :i, :v 7} {:k :i, :v 8}])
         (freeze [{:k :l, :v 9} nil])
         (freeze [{:k :l, :v 11} nil])
         (freeze [nil {:k :r, :v 10}])
         (freeze [nil {:k :r, :v 12}])})))

(deftest test-filter-by
  (let [data (io/return [{:k nil, :v 1}
                         {:k nil, :v 3}
                         {:k :i, :v 5}
                         {:k :i, :v 7}
                         {:k :l, :v 9}
                         {:k :l, :v 11}])]

    (testing "Normal"
      (let [keys (io/return [:i])]
        (test-diff
          (set (exec/debug-script (pig-join/filter-by :k keys data)))
          '#{(freeze {:k :i, :v 5})
             (freeze {:k :i, :v 7})})))
    
    (testing "Nil keys"
      (let [keys (io/return [:i nil])]
        (test-diff
          (set (exec/debug-script (pig-join/filter-by :k keys data)))
          '#{(freeze {:k nil, :v 1})
             (freeze {:k nil, :v 3})
             (freeze {:k :i, :v 5})
             (freeze {:k :i, :v 7})})))
    
    (testing "Duplicate keys"
      (let [keys (io/return [:i :i])]
        (test-diff
          (exec/debug-script (pig-join/filter-by :k keys data))
          '[(freeze {:k :i, :v 5})
            (freeze {:k :i, :v 7})
            (freeze {:k :i, :v 5})
            (freeze {:k :i, :v 7})])))))

(deftest test-remove-by
  (let [data (io/return [{:k nil, :v 1}
                         {:k nil, :v 3}
                         {:k :i, :v 5}
                         {:k :i, :v 7}
                         {:k :l, :v 9}
                         {:k :l, :v 11}])]

    (testing "Normal"
      (let [keys (io/return [:i])]
          (test-diff
            (set (exec/debug-script (pig-join/remove-by :k keys data)))
            '#{(freeze {:k nil, :v 1})
               (freeze {:k nil, :v 3})
               (freeze {:k :l, :v 9})
               (freeze {:k :l, :v 11})})))
    
    (testing "Nil keys"
      (let [keys (io/return [:i nil])]
        (test-diff
          (set (exec/debug-script (pig-join/remove-by :k keys data)))
          '#{(freeze {:k :l, :v 9})
             (freeze {:k :l, :v 11})})))
    
    (testing "Duplicate keys"
      (let [keys (io/return [:i :i])]
        (test-diff
          (set (exec/debug-script (pig-join/remove-by :k keys data)))
          '#{(freeze {:k nil, :v 1})
             (freeze {:k nil, :v 3})
             (freeze {:k :l, :v 9})
             (freeze {:k :l, :v 11})})))))
