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
  (:use clojure.test
        pigpen.test-util)
  (:require [pigpen.code :as pig]))

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
               ([a b] nil)
               ([a b c & more] nil))]
      (pig/assert-arity f 0)
      (pig/assert-arity f 1)
      (pig/assert-arity f 2)
      (is (thrown? java.lang.AssertionError (pig/assert-arity f 3)))
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
  
  (testing "bad fn"
    (is (thrown? clojure.lang.Compiler$CompilerException (pig/assert-arity 'f 0)))
    (is (thrown? java.lang.AssertionError (pig/assert-arity nil 2)))))

(deftest test-trap-locals
  (let [^:local local 2
        foo (fn foo [x]
              (let [y (+ x 1)]
                (pig/trap-locals (fn [z] (+ x y z)))))]
    (is (= (foo 1)
           '(clojure.core/let [y '2 x '1] (fn [z] (+ x y z)))))))
