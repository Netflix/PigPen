(ns pigpen.pig.raw-test
  (:require [clojure.test :refer :all]
            [pigpen.pig.raw :as pig-raw]
            [pigpen.extensions.test :refer [test-diff]]))

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
