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

(ns pigpen.functional-suite
  (:refer-clojure :exclude [read])
  (:require [pigpen.functional-test :refer [all-tests TestHarness]]
            [clojure.test]
            [pigpen.functional.code-test]
            [pigpen.functional.filter-test]
            [pigpen.functional.fold-test]
            [pigpen.functional.io-test]
            [pigpen.functional.join-test]
            [pigpen.functional.map-test]
            [pigpen.functional.set-test]))

(defn suite-test
  "Wraps a test function as a clojure test with a harness. Also registers the
test with the default test suite."
  [name harness test]
  (let [test-var (resolve test)
        test-meta (meta test-var)]
    (assert test-var)
    (assert test-meta)
    `(clojure.test/deftest ~(symbol (str name "-" (clojure.string/replace (ns-name (:ns test-meta)) "." "-") "-" (:name test-meta)))
       (println "Testing:" ~test-var)
       (clojure.test/testing ~(:doc test-meta)
         ((deref ~test-var) ~harness)))))

(defmacro def-functional-tests
  "Defines a suite of functional tests. Provide a name to qualify the test names,
an instance of TestHarness, and optionally a list of tests to run (a list of
namespace qualified symbols)."
  ([name harness]
    `(def-functional-tests ~name ~harness #{} ~(deref all-tests)))
  ([name harness blacklist]
    `(def-functional-tests ~name ~harness ~blacklist ~(deref all-tests)))
  ([name harness blacklist tests]
    (satisfies? TestHarness harness)
    `(do
       ~@(for [test tests
               :when (not (blacklist test))]
           (suite-test name harness test)))))
