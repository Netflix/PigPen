(ns pigpen.cascading-test
  (:use clojure.test pigpen.cascading)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]))

(deftest test-taps
  (println (command->flowdef '{:type     :load
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
  (let [load-cmd (load-text "the/location")]
    (println "load-cmd:" load-cmd)
    (println (command->flowdef load-cmd {}))))

(deftest test-commands->flow
  (println (commands->flow [(load-text "the/location1")
                           (load-text "the/location2")])))
