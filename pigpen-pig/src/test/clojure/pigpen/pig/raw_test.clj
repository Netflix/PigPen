;;
;;
;;  Copyright 2015 Netflix, Inc.
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

(ns pigpen.pig.raw-test
  (:require [clojure.test :refer :all]
            [pigpen.pig.raw :as pig-raw]
            [pigpen.extensions.test :refer [test-diff pigsym-zero]]))

(deftest test-register$

  (test-diff
    (pig-raw/register$ "foo")
    '{:type :register
      :jar "foo"})

  (is (thrown? AssertionError (pig-raw/register$ nil)))
  (is (thrown? AssertionError (pig-raw/register$ 123)))
  (is (thrown? AssertionError (pig-raw/register$ 'foo)))
  (is (thrown? AssertionError (pig-raw/register$ :foo))))

(deftest test-option$

  (test-diff
    (pig-raw/option$ "foo" 123)
    '{:type :option
      :option "foo"
      :value 123}))

(deftest test-return-debug$
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (pig-raw/return-debug$ [{'value "foo"}])
      '{:type :return-debug
        :id return-debug0
        :fields [value]
        :data [{value "foo"}]})))
