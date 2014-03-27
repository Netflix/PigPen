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

(ns pigpen.filter
  "Commands that remove elements from a relation.

  Note: Most of these are present in pigpen.core. Normally you should use those instead.
"
  (:refer-clojure :exclude [filter remove take])
  (:require [pigpen.util :as util]
            [pigpen.raw :as raw]
            [pigpen.code :as code]))

(set! *warn-on-reflection* true)

(defn filter*
  "See #'pigpen.core/filter"
  [pred opts relation]
  {:pre [(map? relation) pred]}
  (code/assert-arity pred (-> relation :fields count))
  (raw/bind$ relation `(pigpen.pig/filter->bind ~pred) opts))

(defmacro filter
  "Returns a relation that only contains the items for which (pred item)
returns true.

  Example:

    (pig/filter even? foo)
    (pig/filter (fn [x] (even? (* x x))) foo)

  See also: pigpen.core/remove, pigpen.core/take, pigpen.core/sample,
            pigpen.core/distinct, pigpen.core/filter-by
"
  [pred relation]
  `(filter* (code/trap ~pred) {:description ~(util/pp-str pred)} ~relation))

(defmacro remove
  "Returns a relation without items for which (pred item) returns true.

  Example:

    (pig/remove even? foo)
    (pig/remove (fn [x] (even? (* x x))) foo)

  See also: pigpen.core/filter, pigpen.core/take, pigpen.core/sample,
            pigpen.core/distinct, pigpen.core/remove-by
"
  [pred relation]
  `(filter (complement ~pred) ~relation))

(defn take
  "Limits the number of records to n items.

  Example:

    (pig/take 200 foo)

  Note: This is potentially an expensive operation when run on the server.

  See also: pigpen.core/filter, pigpen.core/sample
"
  [n relation]
  (raw/limit$ relation n {}))

(defn sample
  "Samples the input records by p percentage. This is non-deterministic;
different values may selected on subsequent runs. p should be a value
between 0.0 and 1.0

  Example:

    (pig/sample 0.01 foo)

  Note: This is potentially an expensive operation when run locally.

  See also: pigpen.core/filter, pigpen.core/take
"
  [p relation]
  (raw/sample$ relation p {}))
