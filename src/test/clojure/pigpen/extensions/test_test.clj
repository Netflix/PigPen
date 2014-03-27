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

(ns pigpen.extensions.test-test
  (:use clojure.test
        pigpen.extensions.test))

;; TODO how to test a failure?
(deftest test-test-diff
  (is (= (test-diff {:a 1 :b ["foo" "bar"]}
                    {:a 1 :b ["foo" "bar"]})
         true)))

(deftest test-pigsym-zero
  (is (= (pigsym-zero "foo") 'foo0))
  (is (= (pigsym-zero "foo") 'foo0))
  (is (= (pigsym-zero "foo") 'foo0)))

(deftest test-pigsym-inc
  (let [ps0 (pigsym-inc)
        ps1 (pigsym-inc)]
    (is (= (ps0 "foo") 'foo1))
    (is (= (ps1 "foo") 'foo1))
    (is (= (ps0 "foo") 'foo2))
    (let [ps2 (pigsym-inc)]
      (is (= (ps0 "foo") 'foo3))
      (is (= (ps2 "foo") 'foo1))
      (is (= (ps1 "foo") 'foo2)))))

(deftest test-regex->string
  (is (= (regex->string {:a ['foo "bar" #"baz"]})
         {:a ['foo "bar" "baz"]})))
