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

(ns pigpen.code-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.code :as pig]))

(deftest test-assert-arity
  (testing "core"
    (is (thrown? java.lang.AssertionError (pig/assert-arity 'constantly 0)))
    (pig/assert-arity 'constantly 1)
    (is (thrown? java.lang.AssertionError (pig/assert-arity 'constantly 2)))
    (pig/assert-arity '+ 0)
    (pig/assert-arity '+ 3)
    (pig/assert-arity '+ 9))
  
  (testing "fn"
    (let [f '(fn
               ([] nil)
               ([a] nil)
               ([a b c & more] nil))]
      (pig/assert-arity f 0)
      (pig/assert-arity f 1)
      (is (thrown? java.lang.AssertionError (pig/assert-arity f 2)))
      (pig/assert-arity f 3)
      (pig/assert-arity f 4)
      (pig/assert-arity f 5)))
  
  (testing "inline"
    (let [f '#(vector %)]
      (is (thrown? java.lang.AssertionError (pig/assert-arity f 0)))
      (pig/assert-arity f 1)
      (is (thrown? java.lang.AssertionError (pig/assert-arity f 2))))
    
    (let [f '#(vector %1 %2)]
      (is (thrown? java.lang.AssertionError (pig/assert-arity f 0)))
      (is (thrown? java.lang.AssertionError (pig/assert-arity f 1)))
      (pig/assert-arity f 2)
      (is (thrown? java.lang.AssertionError (pig/assert-arity f 3)))))

  (testing "varargs zero"
    (let [f (fn [& args] nil)]
      (pig/assert-arity f 0)
      (pig/assert-arity f 1)
      (pig/assert-arity f 2)))
  
  (testing "bad fn"
    (is (thrown? clojure.lang.Compiler$CompilerException (pig/assert-arity 'f 0)))
    (is (thrown? java.lang.AssertionError (pig/assert-arity nil 2)))))

(deftest test-build-requires
  (is (= (pig/build-requires [])
         '(clojure.core/require (quote [pigpen.pig]))))
  (is (= (pig/build-requires '[foo])
         '(clojure.core/require (quote [pigpen.pig]))))
  (is (= (pig/build-requires '[pigpen.code])
         '(clojure.core/require (quote [pigpen.pig]) (quote [pigpen.code]))))
  (is (= (pig/build-requires '[pigpen.code pigpen.code-test])
         '(clojure.core/require (quote [pigpen.pig]) (quote [pigpen.code]) (quote [pigpen.code-test])))))

(defn test-fn [& args]
  (apply + args))  

(deftest test-trap
  (let [^:local local 2
        foo (fn foo [x]
              (let [y (+ x 1)]
                (pig/trap 'pigpen.code-test (fn [z] (test-fn x y z)))))
        expr (foo 1)
        expr-fn (eval expr)]
    (is (= expr
           '(pigpen.runtime/with-ns pigpen.code-test
              (clojure.core/let [y (quote 2)
                                 x (quote 1)]
                (fn [z] (test-fn x y z))))))
    (is (= (expr-fn 3)
           6))))

(deftest test-trap-values
  (test-diff
    (pig/trap-values #{:on :by :key-selector}
                     '(:from r0 :on (fn [x] x) :type :required))
    {:from 'r0
     :on `(pigpen.code/trap (~'fn [~'x] ~'x))
     :type :required}))
