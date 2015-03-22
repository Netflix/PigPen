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

(ns pigpen.functional.join-test
  (:require [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff]]
            [pigpen.join :as pig-join]
            [pigpen.fold :as fold]))

(t/deftest test-group-by
  "normal group-by"
  [harness]
  (test-diff
    (->>
      (t/data harness [{:a 1 :b 2}
                       {:a 1 :b 3}
                       {:a 2 :b 4}])
      (pig-join/group-by :a)
      (t/dump harness)
      (set))
    '#{[1 ({:a 1, :b 2} {:a 1, :b 3})]
       [2 ({:a 2, :b 4})]}))

(t/deftest test-group-false
  "make sure false & nil aren't conflated"
  [harness]
  (test-diff
    (->>
      (t/data harness [nil true false])
      (pig-join/group-by identity)
      (t/dump harness)
      (set))
    '#{[nil (nil)]
       [true (true)]
       [false (false)]}))

(t/deftest test-into
  "normal into"
  [harness]
  (test-diff
    (->>
      (t/data harness [2 4 6])
      (pig-join/into [])
      (t/dump harness))
    '[[2 4 6]]))

(t/deftest test-into-empty
  "empty seq returns nothing"
  [harness]
  (test-diff
    (->>
      (t/data harness [])
      (pig-join/into {})
      (t/dump harness))
    '[]))

(t/deftest test-reduce-conj
  "reduce into vector with conj"
  [harness]
  (test-diff
    (->>
      (t/data harness [2 4 6])
      (pig-join/reduce conj [])
      (t/dump harness))
    '[[2 4 6]]))

(t/deftest test-reduce-+
  "reduce with +"
  [harness]
  (test-diff
    (->>
      (t/data harness [2 4 6])
      (pig-join/reduce +)
      (t/dump harness))
    '[12]))

(t/deftest test-reduce-empty
  "reduce empty seq returns nothing"
  [harness]
  (test-diff
    (->>
      (t/data harness [])
      (pig-join/reduce +)
      (t/dump harness))
    '[]))

(def fold-data
  [{:k :foo, :v 1}
   {:k :foo, :v 2}
   {:k :foo, :v 3}
   {:k :bar, :v 4}
   {:k :bar, :v 5}])

(t/deftest test-fold-inline-sum
  "fold/sum, defined inline"
  [harness]
  (test-diff
    (->>
      (t/data harness fold-data)
      (pig-join/group-by :k
                         {:fold (fold/fold-fn +
                                              (fn [acc value]
                                                (+ acc (:v value))))})
      (t/dump harness)
      (set))
    '#{[:foo 6]
       [:bar 9]}))

(t/deftest test-fold-inline-count
  "fold/count, defined inline"
  [harness]
  (test-diff
    (->>
      (t/data harness fold-data)
      (pig-join/group-by :k
                         {:fold (fold/fold-fn (fn ([] 0)
                                                ([a b] (+ a b)))
                                              (fn [acc _] (inc acc)))})
      (t/dump harness)
      (set))
    '#{[:bar 2]
       [:foo 3]}))

(t/deftest test-fold-count
  "fold/count"
  [harness]
  (test-diff
    (->>
      (t/data harness fold-data)
      (pig-join/group-by :k
                         {:fold (fold/count)})
      (t/dump harness)
      (set))
    '#{[:bar 2]
       [:foo 3]}))

(t/deftest test-fold-cogroup-single
  "single fold co-group"
  [harness]
  (let [data0 (t/data harness [{:k :foo, :a 1}
                               {:k :foo, :a 2}
                               {:k :foo, :a 3}
                               {:k :bar, :a 4}
                               {:k :bar, :a 5}])
        data1 (t/data harness [{:k :foo, :b 1}
                               {:k :foo, :b 2}
                               {:k :bar, :b 3}
                               {:k :bar, :b 4}
                               {:k :bar, :b 5}])]
    (test-diff
      (->>
        (pig-join/cogroup [(data0 :on :k, :required true, :fold (->> (fold/map :a) (fold/sum)))
                           (data1 :on :k, :required true)]
                          vector)
        (t/dump harness)
        (set))
      '#{[:foo 6 ({:k :foo, :b 1} {:k :foo, :b 2})]
         [:bar 9 ({:k :bar, :b 3} {:k :bar, :b 4} {:k :bar, :b 5})]})))

(t/deftest test-fold-cogroup-dual
  "dual fold co-group"
  [harness]
  (let [data0 (t/data harness [{:k :foo, :a 1}
                               {:k :foo, :a 2}
                               {:k :foo, :a 3}
                               {:k :bar, :a 4}
                               {:k :bar, :a 5}])
        data1 (t/data harness [{:k :foo, :b 1}
                               {:k :foo, :b 2}
                               {:k :bar, :b 3}
                               {:k :bar, :b 4}
                               {:k :bar, :b 5}])]
    (test-diff
      (->>
        (pig-join/cogroup [(data0 :on :k, :required true, :fold (->> (fold/map :a) (fold/sum)))
                           (data1 :on :k, :required true, :fold (->> (fold/map :b) (fold/sum)))]
                          vector)
        (t/dump harness)
        (set))
      '#{[:foo 6 3]
         [:bar 9 12]})))

(t/deftest test-fold-all-sum
  "fold all records with sum"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2 3 4])
      (pig-join/fold +)
      (t/dump harness))
    '[10]))

(t/deftest test-fold-all-count
  "fold all records with count"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2 3 4])
      (pig-join/fold (fold/count))
      (t/dump harness))
    '[4]))

(t/deftest test-fold-all-empty
  "fold all records, no input, returns nothing"
  [harness]
  (test-diff
    (->>
      (t/data harness [])
      (pig-join/fold (fold/count))
      (t/dump harness))
    '[]))

(def join-data1
  [{:k nil, :v 1}
   {:k nil, :v 3}
   {:k :i, :v 5}
   {:k :i, :v 7}
   {:k :l, :v 9}
   {:k :l, :v 11}])

(def join-data2
  [{:k nil, :v 2}
   {:k nil, :v 4}
   {:k :i, :v 6}
   {:k :i, :v 8}
   {:k :r, :v 10}
   {:k :r, :v 12}])

(t/deftest test-cogroup-inner
  "inner cogroup"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :by :k :type :required)
                         ((t/data harness join-data2) :by :k :type :required)]
                        vector)
      (t/dump harness)
      (set))
    '#{[:i [{:k :i, :v 5} {:k :i, :v 7}] [{:k :i, :v 6} {:k :i, :v 8}]]}))

(t/deftest test-cogroup-left-outer
  "left outer cogroup"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :on :k :type :required)
                         ((t/data harness join-data2) :on :k :type :optional)]
                        vector)
      (t/dump harness)
      (set))
    '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] nil]
       [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
       [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]}))


(t/deftest test-cogroup-right-outer
  "right outer cogroup"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :on :k :type :optional)
                         ((t/data harness join-data2) :on :k :type :required)]
                        vector)
      (t/dump harness)
      (set))
    '#{[nil nil                           [{:k nil, :v 2} {:k nil, :v 4}]]
       [:i  [{:k :i, :v 5} {:k :i, :v 7}] [{:k :i, :v 6} {:k :i, :v 8}]]
       [:r  nil                           [{:k :r, :v 10} {:k :r, :v 12}]]}))

(t/deftest test-cogroup-full-outer
  "full outer cogroup"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :on :k :type :optional)
                         ((t/data harness join-data2) :on :k :type :optional)]
                        vector)
      (t/dump harness)
      (set))
    '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] nil]
       [nil nil                             [{:k nil, :v 2} {:k nil, :v 4}]]
       [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
       [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]
       [:r  nil                             [{:k :r, :v 10} {:k :r, :v 12}]]}))

(t/deftest test-cogroup-inner-join-nils
  "inner cogroup, joining nils"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :on :k :type :required)
                         ((t/data harness join-data2) :on :k :type :required)]
                        vector
                        {:join-nils true})
      (t/dump harness)
      (set))
    '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
       [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]}))

(t/deftest test-cogroup-left-outer-join-nils
  "left outer cogroup, joining nils"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :on :k :type :required)
                         ((t/data harness join-data2) :on :k :type :optional)]
                        vector
                        {:join-nils true})
      (t/dump harness)
      (set))
    '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
       [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
       [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]}))

(t/deftest test-cogroup-right-outer-join-nils
  "right outer cogroup, joining nils"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :on :k :type :optional)
                         ((t/data harness join-data2) :on :k :type :required)]
                        vector
                        {:join-nils true})
      (t/dump harness)
      (set))
    '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
       [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
       [:r  nil                             [{:k :r, :v 10} {:k :r, :v 12}]]}))

(t/deftest test-cogroup-full-outer-join-nils
  "full outer cogroup, joining nils"
  [harness]
  (test-diff
    (->>
      (pig-join/cogroup [((t/data harness join-data1) :on :k :type :optional)
                         ((t/data harness join-data2) :on :k :type :optional)]
                        vector
                        {:join-nils true})
      (t/dump harness)
      (set))
    '#{[nil [{:k nil, :v 1} {:k nil, :v 3}] [{:k nil, :v 2} {:k nil, :v 4}]]
       [:i  [{:k :i, :v 5} {:k :i, :v 7}]   [{:k :i, :v 6} {:k :i, :v 8}]]
       [:l  [{:k :l, :v 9} {:k :l, :v 11}]  nil]
       [:r  nil                             [{:k :r, :v 10} {:k :r, :v 12}]]}))

(t/deftest test-cogroup-self-join
  "cogroup self join"
  [harness]
  (let [data (t/data harness [0 1 2])]
    (test-diff
      (->>
        (pig-join/cogroup [(data)
                           (data)]
                          vector)
        (t/dump harness)
        (set))
      '#{[0 (0) (0)]
         [1 (1) (1)]
         [2 (2) (2)]})))

(t/deftest test-cogroup-self-join+fold
  "cogroup self join, with fold"
  [harness]
  (let [data (t/data harness [0 1 2])]
    (test-diff
      (->>
        (pig-join/cogroup [(data :fold (fold/count))
                           (data :fold (fold/count))]
                          vector)
        (t/dump harness)
        (set))
      '#{[0 1 1]
         [1 1 1]
         [2 1 1]})))

(t/deftest test-cogroup-self-join+left-fold
  "cogroup self join, with one fold, one not"
  [harness]
  (let [data (t/data harness [0 1 2])]
    (test-diff
      (->>
        (pig-join/cogroup [(data :fold (fold/count))
                           (data)]
                          vector)
        (t/dump harness)
        (set))
      '#{[0 1 (0)]
         [1 1 (1)]
         [2 1 (2)]})))

(t/deftest test-cogroup-self-join+right-fold
  "cogroup self join, with one fold, one not"
  [harness]
  (let [data (t/data harness [0 1 2])]
    (test-diff
      (->>
        (pig-join/cogroup [(data)
                           (data :fold (fold/count))]
                          vector)
        (t/dump harness)
        (set))
      '#{[0 (0) 1]
         [1 (1) 1]
         [2 (2) 1]})))

(t/deftest test-join-inner-implicit
  "inner join - implicit :required"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k)
                      ((t/data harness join-data2) :on :k)]
                     vector)
      (t/dump harness)
      (set))
    '#{[{:k :i, :v 5} {:k :i, :v 6}]
       [{:k :i, :v 5} {:k :i, :v 8}]
       [{:k :i, :v 7} {:k :i, :v 6}]
       [{:k :i, :v 7} {:k :i, :v 8}]}))

(t/deftest test-join-inner
  "inner join"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :required)
                      ((t/data harness join-data2) :on :k :type :required)]
                     vector)
      (t/dump harness)
      (set))
    '#{[{:k :i, :v 5} {:k :i, :v 6}]
       [{:k :i, :v 5} {:k :i, :v 8}]
       [{:k :i, :v 7} {:k :i, :v 6}]
       [{:k :i, :v 7} {:k :i, :v 8}]}))

(t/deftest test-join-left-outer
  "left outer join"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :required)
                      ((t/data harness join-data2) :on :k :type :optional)]
                     vector)
      (t/dump harness)
      (set))
    '#{[{:k nil, :v 1} nil]
       [{:k nil, :v 3} nil]
       [{:k :i, :v 5} {:k :i, :v 6}]
       [{:k :i, :v 5} {:k :i, :v 8}]
       [{:k :i, :v 7} {:k :i, :v 6}]
       [{:k :i, :v 7} {:k :i, :v 8}]
       [{:k :l, :v 9} nil]
       [{:k :l, :v 11} nil]}))

(t/deftest test-join-right-outer
  "right outer join"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :optional)
                      ((t/data harness join-data2) :on :k :type :required)]
                     vector)
      (t/dump harness)
      (set))
    '#{[nil {:k nil, :v 2}]
       [nil {:k nil, :v 4}]
       [{:k :i, :v 5} {:k :i, :v 6}]
       [{:k :i, :v 5} {:k :i, :v 8}]
       [{:k :i, :v 7} {:k :i, :v 6}]
       [{:k :i, :v 7} {:k :i, :v 8}]
       [nil {:k :r, :v 10}]
       [nil {:k :r, :v 12}]}))

(t/deftest test-join-full-outer
  "full outer join"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :optional)
                      ((t/data harness join-data2) :on :k :type :optional)]
                     vector)
      (t/dump harness)
      (set))
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

(t/deftest test-join-inner-join-nils
  "inner join, join nils"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :required)
                      ((t/data harness join-data2) :on :k :type :required)]
                     vector
                     {:join-nils true})
      (t/dump harness)
      (set))
    '#{[{:k nil, :v 1} {:k nil, :v 2}]
       [{:k nil, :v 3} {:k nil, :v 2}]
       [{:k nil, :v 1} {:k nil, :v 4}]
       [{:k nil, :v 3} {:k nil, :v 4}]
       [{:k :i, :v 5} {:k :i, :v 6}]
       [{:k :i, :v 5} {:k :i, :v 8}]
       [{:k :i, :v 7} {:k :i, :v 6}]
       [{:k :i, :v 7} {:k :i, :v 8}]}))

(t/deftest test-join-left-outer-join-nils
  "left outer join, join nils"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :required)
                      ((t/data harness join-data2) :on :k :type :optional)]
                     vector
                     {:join-nils true})
      (t/dump harness)
      (set))
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

(t/deftest test-join-right-outer-join-nils
  "right outer join, join nils"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :optional)
                      ((t/data harness join-data2) :on :k :type :required)]
                     vector
                     {:join-nils true})
      (t/dump harness)
      (set))
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

(t/deftest test-join-full-outer-join-nils
  "full outer join, join nils"
  [harness]
  (test-diff
    (->>
      (pig-join/join [((t/data harness join-data1) :on :k :type :optional)
                      ((t/data harness join-data2) :on :k :type :optional)]
                     vector
                     {:join-nils true})
      (t/dump harness)
      (set))
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

(t/deftest test-join-self-join
  "self join"
  [harness]
  (let [data (t/data harness [0 1 2])]
    (test-diff
      (->>
        (pig-join/join [(data)
                        (data)]
                       vector)
        (t/dump harness)
        (set))
      #{[0 0] [1 1] [2 2]})))

(t/deftest test-join-default-key-selector
  "key-selector defaults to identity"
  [harness]
  (let [data1 (t/data harness [1 2])
        data2 (t/data harness [2 3])]
    (test-diff
      (->>
        (pig-join/join [(data1)
                        (data2)]
                       vector)
        (t/dump harness)
        (set))
      '#{[2 2]})))

(t/deftest test-filter-by
  "normal filter-by"
  [harness]
  (let [keys (t/data harness [:i])]
    (test-diff
      (->>
        (t/data harness join-data1)
        (pig-join/filter-by :k keys)
        (t/dump harness)
        (set))
      '#{{:k :i, :v 5}
         {:k :i, :v 7}})))

(t/deftest test-filter-by-nil-keys
  "normal filter-by with nil keys"
  [harness]
  (let [keys (t/data harness [:i nil])]
    (test-diff
      (->>
        (t/data harness join-data1)
        (pig-join/filter-by :k keys)
        (t/dump harness)
        (set))
      '#{{:k nil, :v 1}
         {:k nil, :v 3}
         {:k :i, :v 5}
         {:k :i, :v 7}})))

(t/deftest test-filter-by-duplicate-keys
  "normal filter-by with duplicate keys"
  [harness]
  (let [keys (t/data harness [:i :i])]
    (test-diff
      (->>
        (t/data harness join-data1)
        (pig-join/filter-by :k keys)
        (t/dump harness)
        (sort-by :v))
      '[{:k :i, :v 5}
        {:k :i, :v 5}
        {:k :i, :v 7}
        {:k :i, :v 7}])))

(t/deftest test-remove-by
  "normal remove-by"
  [harness]
  (let [keys (t/data harness [:i])]
    (test-diff
      (->>
        (t/data harness join-data1)
        (pig-join/remove-by :k keys)
        (t/dump harness)
        (set))
      '#{{:k nil, :v 1}
         {:k nil, :v 3}
         {:k :l, :v 9}
         {:k :l, :v 11}})))

(t/deftest test-remove-by-nil-keys
  "normal remove-by with nil keys"
  [harness]
  (let [keys (t/data harness [:i nil])]
    (test-diff
      (->>
        (t/data harness join-data1)
        (pig-join/remove-by :k keys)
        (t/dump harness)
        (set))
      '#{{:k :l, :v 9}
         {:k :l, :v 11}})))

(t/deftest test-remove-by-duplicate-keys
  "normal remove-by with duplicate keys"
  [harness]
  (let [keys (t/data harness [:i :i])]
    (test-diff
      (->>
        (t/data harness join-data1)
        (pig-join/remove-by :k keys)
        (t/dump harness)
        (set))
      '#{{:k nil, :v 1}
         {:k nil, :v 3}
         {:k :l, :v 9}
         {:k :l, :v 11}})))
