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

(ns pigpen.map
  "Commands to transform data.

  Note: Most of these are present in pigpen.core. Normally you should use those instead.
"
  (:refer-clojure :exclude [map mapcat map-indexed sort sort-by])
  (:require [pigpen.extensions.core :refer [pp-str]]
            [pigpen.raw :as raw]
            [pigpen.code :as code]))

(set! *warn-on-reflection* true)

(defn map*
  "See pigpen.core/map"
  ([f relation]
    (map* f {} relation))
  ([f opts relation]
    {:pre [(map? relation) f]}
    (code/assert-arity f (-> relation :fields count))
    (raw/bind$ `(pigpen.runtime/map->bind ~f) opts relation)))

(defmacro map
  "Returns a relation of f applied to every item in the source relation.
Function f should be a function of one argument.

  Example:

    (pig/map inc foo)
    (pig/map (fn [x] (* x x)) foo)

  Note: Unlike clojure.core/map, pigpen.core/map takes only one relation. This
is due to the fact that there is no defined order in pigpen. See pig/join,
pig/cogroup, and pig/union for combining sets of data.

  See also: pigpen.core/mapcat, pigpen.core/map-indexed, pigpen.core/join,
            pigpen.core/cogroup, pigpen.core/union
"
  {:added "0.1.0"}
  [f relation]
  `(map* (code/trap ~f) {:description ~(pp-str f)} ~relation))

(defn mapcat*
  "See pigpen.core/mapcat"
  ([f relation]
    (mapcat* f {} relation))
  ([f opts relation]
    {:pre [(map? relation) f]}
    (code/assert-arity f (-> relation :fields count))
    (raw/bind$ `(pigpen.runtime/mapcat->bind ~f) opts relation)))

(defmacro mapcat
  "Returns the result of applying concat, or flattening, the result of applying
f to each item in relation. Thus f should return a collection.

  Example:

    (pig/mapcat (fn [x] [(dec x) x (inc x)]) foo)

  See also: pigpen.core/map, pigpen.core/map-indexed
"
  {:added "0.1.0"}
  [f relation]
  `(mapcat* (code/trap ~f) {:description ~(pp-str f)} ~relation))

(defn map-indexed*
  ([f relation]
    (map-indexed* f {} relation))
  ([f opts relation]
    {:pre [(map? relation) f]}
    (code/assert-arity f 2)
    (->> relation
      (raw/rank$ opts)
      (raw/bind$ `(pigpen.runtime/map->bind ~f) {}))))

(defmacro map-indexed
  "Returns a relation of applying f to the the index and value of every item in
the source relation. Function f should be a function of two arguments: the index
and the value. If you require sequential ids, use option {:dense true}.

  Example:

    (pig/map-indexed (fn [i x] (* i x)) foo)
    (pig/map-indexed vector {:dense true} foo)

  Options:

    :dense - force sequential ids (pig only)

  Note: If you require sorted data, use sort or sort-by immediately before
        this command.

  Note: Pig will assign the same index to any equal values, regardless of how
        many times they appear.

  Note: The cascading implementation of map-indexed uses a single reducer

  See also: pigpen.core/sort, pigpen.core/sort-by, pigpen.core/map, pigpen.core/mapcat
"
  {:added "0.1.0"}
  ([f relation] `(map-indexed ~f {} ~relation))
  ([f opts relation]
    `(map-indexed* (code/trap ~f) (assoc ~opts :description ~(pp-str f)) ~relation)))

(defn sort*
  "See pigpen.core/sort, pigpen.core/sort-by"
  ([key-selector comp relation]
    (sort* key-selector comp {} relation))
  ([key-selector comp opts relation]
    {:pre [(map? relation) (#{:asc :desc} comp)]}
    (->> relation
      (raw/bind$ `(pigpen.runtime/key-selector->bind ~key-selector)
                 {:field-type :native-key-frozen-val
                  :alias ['key 'value]})
      (raw/sort$ 'key comp opts))))

(defmacro sort
  "Sorts the data with an optional comparator. Takes an optional map of options.

  Example:

    (pig/sort foo)
    (pig/sort :desc foo)
    (pig/sort :desc {:parallel 20} foo)

  Notes:
    The default comparator is :asc (ascending sort order).
    Only :asc and :desc are supported comparators.
    The values must be primitive values (string, int, etc).
    Maps, vectors, etc are not supported.

  Options:

    :parallel - The degree of parallelism to use (pig only)

  Note: The cascading implementation of sort uses a single reducer

  See also: pigpen.core/sort-by
"
  {:added "0.1.0"}
  ([relation] `(sort :asc {} ~relation))
  ([comp relation] `(sort ~comp {} ~relation))
  ([comp opts relation]
    `(sort* `identity '~comp ~opts ~relation)))

(defmacro sort-by
  "Sorts the data by the specified key-fn with an optional comparator. Takes an
optional map of options.

  Example:

    (pig/sort-by :a foo)
    (pig/sort-by #(count %) :desc foo)
    (pig/sort-by (fn [x] (* x x)) :desc {:parallel 20} foo)

  Notes:
    The default comparator is :asc (ascending sort order).
    Only :asc and :desc are supported comparators.
    The key-fn values must be primitive values (string, int, etc).
    Maps, vectors, etc are not supported.

  Options:

    :parallel - The degree of parallelism to use (pig only)

  Note: The cascading implementation of sort-by uses a single reducer

  See also: pigpen.core/sort
"
  {:added "0.1.0"}
  ([key-fn relation] `(sort-by ~key-fn :asc {} ~relation))
  ([key-fn comp relation] `(sort-by ~key-fn ~comp {} ~relation))
  ([key-fn comp opts relation]
    `(sort* (code/trap ~key-fn)
            '~comp
            (assoc ~opts :description ~(pp-str key-fn))
            ~relation)))
