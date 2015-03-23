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

(ns pigpen.pig.runtime-test
  (:require [clojure.test :refer :all]
            [pigpen.runtime :as rt]
            [pigpen.pig.runtime :refer :all]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [taoensso.nippy :refer [freeze thaw]]
            [clojure.core.async :as a]
            [pigpen.extensions.core-async :as ae])
  (:import [org.apache.pig.data
            DataByteArray
            Tuple TupleFactory
            DataBag BagFactory]
           [java.util Map]))

;; TODO test-tuple
;; TODO test-bag

; *****************

(deftest test-cast-bytes

  (testing "nil"
    (is (= nil (cast-bytes nil nil))))

  (let [b (byte-array (mapv byte [97 98 99]))]
    (testing "bytearray"
      (is (= b (cast-bytes "bytearray" b))))

    (testing "chararray"
      (is (= "abc" (cast-bytes "chararray" b))))))

; *****************

(deftest test-hybrid->clojure

  (testing "DataByteArray"
    (is (= (rt/hybrid->clojure (DataByteArray. (freeze {:a [1 2 3]})))
           {:a [1 2 3]})))

  (testing "Tuple"
    (is (= (rt/hybrid->clojure (tuple (DataByteArray. (freeze "foo"))
                                   (DataByteArray. (freeze "bar"))))
           ["foo" "bar"])))

  (testing "DataBag"
    (is (= (rt/hybrid->clojure (bag (tuple (DataByteArray. (freeze "foo")))))
           ["foo"]))))

; *****************

;; TODO test-native->clojure

; *****************

(deftest test-bag->chan
  (let [c (a/chan 5)
        b (bag (tuple 1) (tuple "a"))]
    (#'pigpen.pig.runtime/bag->chan c b)
    (is (= (a/<!! c) 1))
    (is (= (a/<!! c) "a"))))

(deftest test-lazy-bag-args
  (let [b (bag (tuple 1) (tuple "a"))
        args [b 2 "b"]
        [args* input-bags] (#'pigpen.pig.runtime/lazy-bag-args args)
        [a0 a1 a2] args*
        [i0 i1 i2] input-bags]
    (is (ae/channel? a0))
    (is (= a1 2))
    (is (= a2 "b"))
    (is (ae/channel? i0))
    (is (nil? i1))
    (is (nil? i2))))

(deftest test-create-accumulate-state

  (let [b (bag (tuple 1) (tuple "a"))
        t (tuple b 2 "b")
        [input-bags result] (#'pigpen.pig.runtime/create-accumulate-state
                              (fn [[x y z]] [(a/<!! x) y z])
                              t)
        [i0 i1 i2] input-bags]
    (is (ae/channel? i0))
    (is (nil? i1))
    (is (nil? i2))
    (is (ae/channel? result))))

(deftest test-accumulate

  (require '[clojure.core.async :as a])

  (testing "1 bag"
    (let [t (tuple (apply bag (map tuple (range 100))) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] [(a/<!! x) y z])
                                nil t)
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [(range 100) 2 "b"]))
      (is (nil? state))))

  (testing "2 bags"
    (let [t (tuple (apply bag (map tuple (range 100))) 2 "b")
          t' (tuple (apply bag (map tuple (range 100 200))) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] [(a/<!! x) y z])
                                nil t)
          state (udf-accumulate (fn [_ [x y z]] [(a/<!! x) y z])
                                state t')
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [(range 200) 2 "b"]))
      (is (nil? state))))

  (testing "2 bag args"
    (let [t (tuple (bag (tuple 1) (tuple 2) (tuple 3)) (bag (tuple 4) (tuple 5) (tuple 6)))
          t' (tuple (bag (tuple 7) (tuple 8) (tuple 9)) (bag (tuple 10) (tuple 11) (tuple 12)))
          state (udf-accumulate (fn [_ [x y]] [(a/<!! x) (a/<!! y)])
                                nil t)
          state (udf-accumulate (fn [_ [x y]] [(a/<!! x) (a/<!! y)])
                                state t')
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [[1 2 3 7 8 9] [4 5 6 10 11 12]]))
      (is (nil? state))))

  (testing "empty bag"
    (let [t (tuple (bag) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] [(a/<!! x) y z])
                                nil t)
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [[] 2 "b"]))
      (is (nil? state))))

  (testing "single value bag"
    (let [t (tuple (bag (tuple 1)) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] [(a/<!! x) y z])
                                nil t)
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [[1] 2 "b"]))
      (is (nil? state))))

  (testing "all values"
    (let [t (tuple 1 2 "b")
          state (udf-accumulate (fn [_ [x y z]] [x y z])
                                nil t)
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [1 2 "b"]))
      (is (nil? state))))

  (testing "nil in bag"
    (let [t (tuple (bag (tuple 1) (tuple nil) (tuple 3)) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] [(a/<!! x) y z])
                                nil t)
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [[1 nil 3] 2 "b"]))
      (is (nil? state))))

  (testing "nil value"
    (let [t (tuple (bag) nil "b")
          state (udf-accumulate (fn [_ [x y z]] [(a/<!! x) y z])
                                nil t)
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result [[] nil "b"]))
      (is (nil? state))))

  (testing "nil result"
    (let [t (tuple (apply bag (map tuple (range 100))) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] nil)
                                nil t)
          result (udf-get-value state)
          state (udf-cleanup state)]
      (is (= result nil))
      (is (nil? state))))

  (testing "throw in agg"
    (let [t (tuple (bag (tuple "1") (tuple "a") (tuple "3")) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] [(mapv #(java.lang.Long/valueOf %) (a/<!! x)) y z])
                                nil t)]
      (is (thrown? RuntimeException (udf-get-value state)))
      (is (nil? (udf-cleanup state)))))

  (testing "throw in udf"
    (let [t (tuple (bag) 2 "b")
          state (udf-accumulate (fn [_ [x y z]] (throw (Exception.)))
                                nil t)]
      (is (thrown? Exception (udf-get-value state)))
      (is (nil? (udf-cleanup state))))))

; *****************

;; TODO test serialization equivalency
;; TODO test serialization round-trip

