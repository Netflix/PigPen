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

(ns pigpen.functional.filter-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff]]
            [pigpen.core :as pig]
            [pigpen.pig :refer [dump]]
            [pigpen.fold :as fold]))

(deftest test-filter
  
  (let [data (pig/return [1 2])
        command (pig/filter odd? data)]
    (test-diff
      (dump command)
      '[1])))

(deftest test-remove
  
  (let [data (pig/return [1 2])
        command (pig/remove odd? data)]
    (test-diff
      (dump command)
      '[2])))
