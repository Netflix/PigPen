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

(ns pigpen.functional.join-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff]]
            [pigpen.core :as pig]
            [pigpen.fold :as fold]))

(deftest test-group-by
  (let [data (pig/return [{:a 1 :b 2}
                         {:a 1 :b 3}
                         {:a 2 :b 4}])

        command (pig/group-by :a data)]

    (test-diff
      (set (pig/dump command))
      '#{[1 ({:a 1, :b 2} {:a 1, :b 3})]
         [2 ({:a 2, :b 4})]})))

(deftest test-into
  (let [data (pig/return [2 4 6])
        command (pig/into [] data)]

    (test-diff
      (pig/dump command)
      '[[2 4 6]]))

  (testing "empty seq returns nothing"
    (let [data (pig/return [])
          command (pig/into {} data)]

      (test-diff
        (pig/dump command)
        '[]))))

(deftest test-reduce
  (testing "conj"
    (let [data (pig/return [2 4 6])
          command (pig/reduce conj [] data)]

      (test-diff
        (pig/dump command)
        '[[2 4 6]])))

  (testing "+"
    (let [data (pig/return [2 4 6])
          command (pig/reduce + data)]

      (test-diff
        (pig/dump command)
        '[12])))

  (testing "empty seq returns nothing"
    (let [data (pig/return [])
          command (pig/reduce + data)]

      (test-diff
        (pig/dump command)
        '[]))))

(deftest test-fold
  (let [data (pig/return [{:k :foo, :v 1}
                          {:k :foo, :v 2}
                          {:k :foo, :v 3}
                          {:k :bar, :v 4}
                          {:k :bar, :v 5}])]

    (testing "inline sum"
      (let [command (->> data
                      (pig/group-by :k
                        {:fold (fold/fold-fn +
                                             (fn [acc value]
                                               (+ acc (:v value))))}))]
        (is (= (set (pig/dump command))
               '#{[:foo 6]
                  [:bar 9]}))))

    (testing "inline count"
      (let [command (->> data
                      (pig/group-by :k
                        {:fold (fold/fold-fn (fn ([] 0)
                                                 ([a b] (+ a b)))
                                             (fn [acc _] (inc acc)))}))]
        (is (= (set (pig/dump command))
               '#{[:bar 2]
                  [:foo 3]}))))

    (testing "using fold/count"
      (let [command (->> data
                      (pig/group-by :k
                        {:fold (fold/count)}))]
        (is (= (set (pig/dump command))
               '#{[:bar 2]
                  [:foo 3]})))))

  (testing "dual fold co-group"
    (let [data0 (pig/return [{:k :foo, :a 1}
                            {:k :foo, :a 2}
                            {:k :foo, :a 3}
                            {:k :bar, :a 4}
                            {:k :bar, :a 5}])
          data1 (pig/return [{:k :foo, :b 1}
                            {:k :foo, :b 2}
                            {:k :bar, :b 3}
                            {:k :bar, :b 4}
                            {:k :bar, :b 5}])
          command (pig/cogroup [(data0 :on :k, :required true, :fold (->> (fold/map :a) (fold/sum)))
                                (data1 :on :k, :required true, :fold (->> (fold/map :b) (fold/sum)))]
                               vector)]
      (is (= (set (pig/dump command))
             '#{[:foo 6 3]
                [:bar 9 12]}))))

  (testing "fold all sum"
    (let [data (pig/return [1 2 3 4])
          command (pig/fold + data)]
      (is (= (pig/dump command)
             '[10]))))

  (testing "fold all count"
    (let [data (pig/return [1 2 3 4])
          command (pig/fold (fold/count) data)]
     (is (= (pig/dump command)
            '[4]))))

  (testing "empty seq returns nothing"
    (let [data (pig/return [])
          command (pig/fold (fold/count) data)]
     (is (= (pig/dump command)
            '[])))))

(deftest test-cogroup
  (let [data1 (pig/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (pig/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])]

    (testing "inner"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :by :k :type :required)
                                     (data2 :by :k :type :required)]
                                    vector)))
        '#{[:i [{:k :i, :v 5} {:k :i, :v 7}] [{:k :i, :v 6} {:k :i, :v 8}]]}))

    (testing "left outer"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :on :k :type :required)
                                     (data2 :on :k :type :optional)]
                                    vector)))
        '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] nil]
           [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
           [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]}))

    (testing "right outer"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :on :k :type :optional)
                                     (data2 :on :k :type :required)]
                                    vector)))
        '#{[nil nil                           [{:k nil, :v 2} {:k nil, :v 4}]]
           [:i  [{:k :i, :v 5} {:k :i, :v 7}] [{:k :i, :v 6} {:k :i, :v 8}]]
           [:r  nil                           [{:k :r, :v 10} {:k :r, :v 12}]]}))

    (testing "full outer"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :on :k :type :optional)
                                     (data2 :on :k :type :optional)]
                                    vector)))
        '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] nil]
           [nil nil                             [{:k nil, :v 2} {:k nil, :v 4}]]
           [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
           [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]
           [:r  nil                             [{:k :r, :v 10} {:k :r, :v 12}]]}))

    (testing "inner join nils"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :on :k :type :required)
                                     (data2 :on :k :type :required)]
                                    vector
                                    {:join-nils true})))
        '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
           [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]}))

    (testing "left outer join nils"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :on :k :type :required)
                                     (data2 :on :k :type :optional)]
                                    vector
                                    {:join-nils true})))
        '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
           [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
           [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]}))

    (testing "right outer join nils"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :on :k :type :optional)
                                     (data2 :on :k :type :required)]
                                    vector
                                    {:join-nils true})))
        '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
           [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
           [:r  nil                             [{:k :r, :v 10} {:k :r, :v 12}]]}))

    (testing "full outer join nils"
      (test-diff
        (set (pig/dump (pig/cogroup [(data1 :on :k :type :optional)
                                     (data2 :on :k :type :optional)]
                                    vector
                                    {:join-nils true})))
        '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
           [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
           [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]
           [:r  nil                             [{:k :r, :v 10} {:k :r, :v 12}]]}))

    (testing "self cogroup"
      (let [data (pig/return [0 1 2])
          command (pig/cogroup [(data)
                                (data)]
                               vector)]
        (is (= (set (pig/dump command))
               '#{[2 (2) (2)] [0 (0) (0)] [1 (1) (1)]}))))

    (testing "self cogroup with fold"
      (let [data (pig/return [0 1 2])
          command (pig/cogroup [(data :fold (fold/count))
                                (data :fold (fold/count))]
                               vector)]
        (is (= (set (pig/dump command))
               '#{[2 1 1] [0 1 1] [1 1 1]}))))))

(deftest test-join
  (let [data1 (pig/return [{:k nil, :v 1}
                          {:k nil, :v 3}
                          {:k :i, :v 5}
                          {:k :i, :v 7}
                          {:k :l, :v 9}
                          {:k :l, :v 11}])
        data2 (pig/return [{:k nil, :v 2}
                          {:k nil, :v 4}
                          {:k :i, :v 6}
                          {:k :i, :v 8}
                          {:k :r, :v 10}
                          {:k :r, :v 12}])]

    (testing "inner join - implicit :required"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k)
                                  (data2 :on :k)]
                                 vector)))
        '#{[{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]}))

    (testing "inner"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :required)
                                  (data2 :on :k :type :required)]
                                 vector)))
        '#{[{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]}))

    (testing "left outer"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :required)
                                  (data2 :on :k :type :optional)]
                                 vector)))
        '#{[{:k nil, :v 1} nil]
           [{:k nil, :v 3} nil]
           [{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]
           [{:k :l, :v 9} nil]
           [{:k :l, :v 11} nil]}))

    (testing "right outer"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :optional)
                                  (data2 :on :k :type :required)]
                                 vector)))
        '#{[nil {:k nil, :v 2}]
           [nil {:k nil, :v 4}]
           [{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]
           [nil {:k :r, :v 10}]
           [nil {:k :r, :v 12}]}))

    (testing "full outer"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :optional)
                                  (data2 :on :k :type :optional)]
                                 vector)))
        '#{[{:k nil, :v 1} nil]
           [{:k nil, :v 3} nil]
           [nil {:k nil, :v 2}]
           [nil {:k nil, :v 4}]
           [{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]
           [{:k :l, :v 9} nil]
           [{:k :l, :v 11} nil]
           [nil {:k :r, :v 10}]
           [nil {:k :r, :v 12}]}))

    (testing "inner join nils"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :required)
                                  (data2 :on :k :type :required)]
                                 vector
                                 {:join-nils true})))
        '#{[{:k nil, :v 1} {:k nil, :v 2}]
           [{:k nil, :v 3} {:k nil, :v 2}]
           [{:k nil, :v 1} {:k nil, :v 4}]
           [{:k nil, :v 3} {:k nil, :v 4}]
           [{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]}))

    (testing "left outer join nils"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :required)
                                  (data2 :on :k :type :optional)]
                                 vector
                                 {:join-nils true})))
        '#{[{:k nil, :v 1} {:k nil, :v 2}]
           [{:k nil, :v 3} {:k nil, :v 2}]
           [{:k nil, :v 1} {:k nil, :v 4}]
           [{:k nil, :v 3} {:k nil, :v 4}]
           [{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]
           [{:k :l, :v 9} nil]
           [{:k :l, :v 11} nil]}))

    (testing "right outer join nils"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :optional)
                                  (data2 :on :k :type :required)]
                                 vector
                                 {:join-nils true})))
        '#{[{:k nil, :v 1} {:k nil, :v 2}]
           [{:k nil, :v 3} {:k nil, :v 2}]
           [{:k nil, :v 1} {:k nil, :v 4}]
           [{:k nil, :v 3} {:k nil, :v 4}]
           [{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]
           [nil {:k :r, :v 10}]
           [nil {:k :r, :v 12}]}))

    (testing "full outer join nils"
      (test-diff
        (set (pig/dump (pig/join [(data1 :on :k :type :optional)
                                  (data2 :on :k :type :optional)]
                                 vector
                                 {:join-nils true})))
        '#{[{:k nil, :v 1} {:k nil, :v 2}]
           [{:k nil, :v 3} {:k nil, :v 2}]
           [{:k nil, :v 1} {:k nil, :v 4}]
           [{:k nil, :v 3} {:k nil, :v 4}]
           [{:k :i, :v 5} {:k :i, :v 6}]
           [{:k :i, :v 5} {:k :i, :v 8}]
           [{:k :i, :v 7} {:k :i, :v 6}]
           [{:k :i, :v 7} {:k :i, :v 8}]
           [{:k :l, :v 9} nil]
           [{:k :l, :v 11} nil]
           [nil {:k :r, :v 10}]
           [nil {:k :r, :v 12}]}))

  (testing "self join"
    (let [data (pig/return [0 1 2])
          command (pig/join [(data)
                                  (data)]
                                 vector)]
      (is (= (set (pig/dump command))
             #{[2 2] [0 0] [1 1]}))))

  (testing "key-selector defaults to identity"
    (let [data1 (pig/return [1 2])
          data2 (pig/return [2 3])

          command (pig/join [(data1)
                             (data2)]
                            vector)]
      (test-diff
        (set (pig/dump command))
        '#{[2 2]})))))

(deftest test-filter-by
  (let [data (pig/return [{:k nil, :v 1}
                         {:k nil, :v 3}
                         {:k :i, :v 5}
                         {:k :i, :v 7}
                         {:k :l, :v 9}
                         {:k :l, :v 11}])]

    (testing "Normal"
      (let [keys (pig/return [:i])]
        (test-diff
          (set (pig/dump (pig/filter-by :k keys data)))
          '#{{:k :i, :v 5}
             {:k :i, :v 7}})))

    (testing "Nil keys"
      (let [keys (pig/return [:i nil])]
        (test-diff
          (set (pig/dump (pig/filter-by :k keys data)))
          '#{{:k nil, :v 1}
             {:k nil, :v 3}
             {:k :i, :v 5}
             {:k :i, :v 7}})))

    (testing "Duplicate keys"
      (let [keys (pig/return [:i :i])]
        (test-diff
          (pig/dump (pig/filter-by :k keys data))
          '[{:k :i, :v 5}
            {:k :i, :v 7}
            {:k :i, :v 5}
            {:k :i, :v 7}])))))

(deftest test-remove-by
  (let [data (pig/return [{:k nil, :v 1}
                         {:k nil, :v 3}
                         {:k :i, :v 5}
                         {:k :i, :v 7}
                         {:k :l, :v 9}
                         {:k :l, :v 11}])]

    (testing "Normal"
      (let [keys (pig/return [:i])]
          (test-diff
            (set (pig/dump (pig/remove-by :k keys data)))
            '#{{:k nil, :v 1}
               {:k nil, :v 3}
               {:k :l, :v 9}
               {:k :l, :v 11}})))

    (testing "Nil keys"
      (let [keys (pig/return [:i nil])]
        (test-diff
          (set (pig/dump (pig/remove-by :k keys data)))
          '#{{:k :l, :v 9}
             {:k :l, :v 11}})))

    (testing "Duplicate keys"
      (let [keys (pig/return [:i :i])]
        (test-diff
          (set (pig/dump (pig/remove-by :k keys data)))
          '#{{:k nil, :v 1}
             {:k nil, :v 3}
             {:k :l, :v 9}
             {:k :l, :v 11}})))))
