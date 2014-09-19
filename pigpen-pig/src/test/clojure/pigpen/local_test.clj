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
  (:require [pigpen.extensions.test :refer [test-diff pigsym-inc]]
            [pigpen.local :as local :refer [PigPenLocalLoader]]
            [pigpen.pig :refer [freeze-vals thaw-anything]]
            [pigpen.raw :as raw]
            [pigpen.core :as pig]
            [pigpen.io :as io]
            [pigpen.exec :as exec])
  (:import [org.apache.pig.data DataBag]))

(.mkdirs (java.io.File. "build/local-test"))

(defn debug-script-raw [query]
  (->> query
    exec/query->observable
    local/observable->raw-data))

(defn debug-script [query]
  (map 'value (debug-script-raw query)))

(deftest test-eval-code
  
  (let [code (raw/code$ :normal '[x y]
               (raw/expr$ '(require (quote [pigpen.pig]))
                          '(pigpen.runtime/exec
                             [(pigpen.runtime/process->bind
                                (pigpen.runtime/pre-process :pig :frozen))
                              (pigpen.runtime/map->bind (fn [x y] (+ x y)))
                              (pigpen.runtime/process->bind
                                (pigpen.runtime/post-process :pig :frozen))])))
        values (freeze-vals {'x 37, 'y 42})]
    (is (= (thaw-anything (#'pigpen.local/eval-code code values))
           '((tuple (freeze 79)))))))

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
                    (pig/load-clj "build/local-test/test-debug-in")
                    (pig/filter (comp odd? :a))
                    (pig/map :b)
                    (pig/store-clj "build/local-test/test-debug-out"))]
      (spit "build/local-test/test-debug-in" "{:a 1, :b \"foo\"}\n{:a 2, :b \"bar\"}")
      #_(exec/write-script "build/local-test/temp.pig" {:debug "build/local-test/"} command)
      (exec/dump {:debug "build/local-test/test-debug-"} command)
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
      (debug-script-raw command)
      '[{a "a", b "b", c "c"}
        {a "1", b "2", c "3"}])))

(defmethod local/load "BadStorage" [command]
  (let [fail (get-in command [:opts :fail])]
    (reify PigPenLocalLoader
      (locations [_]
        (if (= fail :locations)
          (throw (Exception. "locations"))
          ["foo" "bar"]))
      (init-reader [_ _]
        (if (= fail :init-reader)
          (throw (Exception. "init-reader"))
          :reader))
      (read [_ _]
        (if (= fail :read)
          (throw (Exception. "read"))
          [{'value 1}
           {'value 2}
           {'value 3}]))
      (close-reader [_ _]
        (when (= fail :close)
          (throw (Exception. "close-reader")))))))

(deftest test-load-exception-handling
  (let [storage (raw/storage$ [] "BadStorage" [])]
    (testing "normal"
      (let [command (raw/load$ "nothing" ['value] storage {:fail nil})]
        (is (= (exec/dump command) [1 2 3 1 2 3]))))
    (testing "fail locations"
      (let [command (raw/load$ "nothing" ['value] storage {:fail :locations})]
        (is (thrown? Exception (exec/dump command)))))
    (testing "fail init-reader"
      (let [command (raw/load$ "nothing" ['value] storage {:fail :init-reader})]
        (is (thrown? Exception (exec/dump command)))))
    (testing "fail read"
      (let [command (raw/load$ "nothing" ['value] storage {:fail :read})]
        (is (thrown? Exception (exec/dump command)))))
    (testing "fail close"
      (let [command (raw/load$ "nothing" ['value] storage {:fail :close})]
        (is (thrown? Exception (exec/dump command)))))))

(deftest test-store
  (let [data (io/return-raw '[{a "a", b "b", c "c"}
                              {a "1", b "2", c "3"}])
        command (raw/store$ data "build/local-test/test-store" raw/default-storage {})]
    (debug-script command)
    (is (= "a\tb\tc\n1\t2\t3\n"
           (slurp "build/local-test/test-store")))))

;; ********** Map **********

(deftest test-exception-handling
  (let [data (io/return [1 2 3])
        command (pig/map (fn [x] (throw (java.lang.Exception.))) data)]
    (is (thrown? Exception (exec/dump command)))))

(deftest test-pig-compare
  (is (= -1 (#'pigpen.local/pig-compare ['key :asc] '{key 1} '{key 2})))
  (is (= 1 (#'pigpen.local/pig-compare ['key :desc] '{key 1} '{key 2})))
  (is (= -1 (#'pigpen.local/pig-compare ['key :desc 'value :asc] '{key 1 value 1} '{key 1 value 2}))))

;; ********** Filter **********

(deftest test-filter-native
  
  (let [data (io/return-raw '[{foo 1, bar 3}
                              {foo 2, bar 1}])]
    
    (testing "with filter"
      (let [command (raw/filter-native$ data '(and (= foo 1) (> bar 2)) {:fields '[foo bar]})]
        (test-diff
          (debug-script-raw command)
          '[{foo 1, bar 3}])))  

    (testing "no filter"
      (let [command' (raw/filter-native$ data nil {:fields '[foo bar]})]
        (test-diff
          (debug-script-raw command')
          '[{foo 1, bar 3}
            {foo 2, bar 1}])))))

(deftest test-limit
  
  (let [data (io/return-raw
               (map freeze-vals '[{x 1, y 2}
                                  {x 2, y 4}]))
        command (raw/limit$ data 1 {})]
    (test-diff
      (debug-script-raw command)
      '[{x (freeze 1), y (freeze 2)}])))

(deftest test-sample
  
  (let [data (io/return (repeat 1000 {:x 1, :y 2}))
        command (raw/sample$ data 0.5 {})]
    (< 490
       (count (debug-script command))
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
      (debug-script command)
      '[(freeze {:y 2, :x 1})
        (freeze {:y 4, :x 2})])))

;; ********** Join **********

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
        (set (debug-script-raw command))
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
        (set (debug-script-raw command))
        #{'{[[return1 key]] (freeze :a), [[return1 value]] (freeze 2), [[return2 key]] (freeze :a), [[return2 value]] (freeze 8)}
          '{[[return1 key]] (freeze :a), [[return1 value]] (freeze 4), [[return2 key]] (freeze :a), [[return2 value]] (freeze 8)}
          '{[[return1 key]] (freeze :b), [[return1 value]] (freeze 6), [[return2 key]] (freeze :b), [[return2 value]] (freeze 10)}
          '{[[return1 key]] (freeze :b), [[return1 value]] (freeze 6), [[return2 key]] (freeze :b), [[return2 value]] (freeze 12)}}))))

