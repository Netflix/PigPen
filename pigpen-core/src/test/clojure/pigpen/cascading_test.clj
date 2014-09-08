(ns pigpen.cascading-test
  (:use clojure.test pigpen.cascading)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]))

(deftest test-taps
  (println (command->flow '{:type     :load
                            :id       load0
                            :location "some/path"
                            :storage  {:type :storage
                                       :func "text"
                                       :args []}
                            :fields   [a b c]
                            :opts     {:type :load-opts}}
                          {}))
  (is (= 1 1)))

(deftest test-load-text
  (println (load-text "the-location")))
