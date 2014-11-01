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
  (:require [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff]]
            [pigpen.core :as pig]
            [pigpen.fold :as fold]))

(t/deftest test-filter
  "normal filter"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2])
      (pig/filter odd?)
      (t/dump harness))
    '[1]))

(t/deftest test-remove
  "normal remove"
  [harness]
  (test-diff
    (->>
      (t/data harness [1 2])
      (pig/remove odd?)
      (t/dump harness))
    '[2]))
