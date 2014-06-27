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

(ns pigpen.extensions.core-test
  (:use clojure.test
        pigpen.extensions.core))

(deftest test-pp-str
  (let [data {:a "very long string" :b "another really really long string" :c [1 2 3]}]
    (is (= (pp-str data)
           "{:a \"very long string\",\n :c [1 2 3],\n :b \"another really really long string\"}\n"))))

(deftest test-zip
  (is (= (zip [x [1 2 3]
               y [:a :b :c]
               z ["foo" "bar" "baz"]]
           [x y z])
         '([1 :a "foo"] [2 :b "bar"] [3 :c "baz"]))))

(deftest test-zipv
  (is (= (zipv [x [1 2 3]
                y [:a :b :c]
                z ["foo" "bar" "baz"]]
           [x y z])
         '[[1 :a "foo"] [2 :b "bar"] [3 :c "baz"]])))

(deftest test-forcat
  (is (= (forcat [x ["a" "b"]
                  y [:foo :bar]]
           [x y (str x y)])
         ["a" :foo "a:foo"
          "a" :bar "a:bar"
          "b" :foo "b:foo"
          "b" :bar "b:bar"])))
