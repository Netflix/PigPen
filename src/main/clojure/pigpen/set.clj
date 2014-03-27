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

(ns pigpen.set
  "Set operations for PigPen.

  Note: Most of these are present in pigpen.core. Normally you should use those instead.
"
  (:refer-clojure :exclude [distinct concat])
  (:require [clojure.set]
            [pigpen.raw :as raw]
            [pigpen.code :as code]))

(set! *warn-on-reflection* true)

(defn ^:private split-opts-relations
  "Identifies optional opts map"
  [opts-relations]
  {:pre [(every? map? opts-relations)]}
  ;; Using the existence of :type and :id as a proxy to indicate a relation.
  ;; Not a huge fan, but can't think of anything better at the time
  ;; TODO Use ^:pig here
  (let [[opts relations] (split-with #(not (and (contains? % :type) (contains? % :id))) opts-relations)]
    [(apply merge opts) relations]))

;; TODO add options to use fold here

(defn ^:private set-operation
  "Common base for most set operations"
  [f opts-relations]
  (let [[opts relations] (split-opts-relations opts-relations)
        keys       (for [r relations] ['value])
        values     (for [r relations] [[(:id r)] 'value])
        join-types (for [r relations] :optional)]
    (-> relations
      (raw/group$ keys join-types opts)
      (raw/bind$ '[pigpen.set] `(pigpen.pig/mapcat->bind ~f) {:args values}))))

(defn pig-intersection
  "Utility method used by #'pigpen.core/intersection"
  [& values]
  (->> values
    (clojure.core/map set)
    (apply clojure.set/intersection)))

(defn pig-intersection-multiset
  "Utility method used by #'pigpen.core/intersection-multiset"
  [& values]
  (apply clojure.core/map (fn [& args] (first args)) values))

(defn pig-difference
  "Utility method used by #'pigpen.core/difference"
  [& values]
  (->> values
    (clojure.core/map set)
    (apply clojure.set/difference)))

(defn pig-difference-multiset
  "Utility method used by #'pigpen.core/difference-multiset"
  [s0 & more]
  (drop (->> more (map count) (reduce +)) s0))

(defn distinct
  "Returns a relation with the distinct values of relation. Optionally takes a
map of options.

  Example:

    (pig/distinct foo)
    (pig/distinct {:parallel 20} foo)

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/union, pigpen.core/union-multiset, pigpen.core/filter
"
  {:added "0.1.0"}
  ([relation] (distinct {} relation))
  ([opts relation]
    (raw/distinct$ relation opts)))

(defn union
  "Performs a union on all relations provided and returns the distinct results.
Optionally takes a map of options as the first parameter.

  Example:

    (pig/union
      (pig/return [1 2 2 3 3 3 4 5])
      (pig/return [1 2 2 3 3])
      (pig/return [1 1 2 2 3 3]))

    => [1 2 3 4 5]

  Options:

    :parallel n - the degree of parallelism to use

  See also: pigpen.core/union-multiset, pigpen.core/distinct
"
  {:arglists '([opts? relations+])
   :added "0.1.0"}
  [& opts-relations]
  (let [[opts relations] (split-opts-relations opts-relations)]
    (raw/distinct$ (raw/union$ (filter identity relations) {}) opts)))

(defn concat
  "Concatenates all relations provided. Does not guarantee any ordering of the
relations. Identical to pigpen.core/union-multiset.

  Example:

    (pig/concat
      (pig/return [1 2 2 3 3 3 4 5])
      (pig/return [1 2 2 3 3])
      (pig/return [1 1 2 2 3 3]))

    => [1 1 1 1 2 2 2 2 2 2 3 3 3 3 3 3 3 4 5]

  See also: pigpen.core/union, pigpen.core/distinct, pigpen.core/union-multiset
"
  {:arglists '([relations+])
   :added "0.1.0"}
  [& relations]
  (raw/union$ (filter identity relations) {}))

(defn union-multiset
  "Performs a union on all relations provided and returns all results.
Identical to pigpen.core/concat.

  Example:

    (pig/union-multiset
      (pig/return [1 2 2 3 3 3 4 5])
      (pig/return [1 2 2 3 3])
      (pig/return [1 1 2 2 3 3]))

    => [1 1 1 1 2 2 2 2 2 2 3 3 3 3 3 3 3 4 5]

  See also: pigpen.core/union, pigpen.core/distinct, pigpen.core/concat
"
  {:arglists '([relations+])
   :added "0.1.0"}
  [& relations]
  (raw/union$ (filter identity relations) {}))

(defn intersection
  "Performs an intersection on all relations provided and returns the distinct
results. Optionally takes a map of options as the first parameter.

  Example:

    (pig/intersection
      (pig/return [1 2 2 3 3 3 4 5])
      (pig/return [1 2 2 3 3])
      (pig/return [1 1 2 2 3 3]))

    => [1 2 3]

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/intersection-multiset, pigpen.core/difference
"
  {:arglists '([opts? relations+])
   :added "0.1.0"}
  [& opts-relations]
  (set-operation 'pigpen.set/pig-intersection opts-relations))

(defn intersection-multiset
  "Performs a multiset intersection on all relations provided and returns all
results. Optionally takes a map of options as the first parameter.

  Example:

    (pig/intersection-multiset
      (pig/return [1 2 2 3 3 3 4 5])
      (pig/return [1 2 2 3 3])
      (pig/return [1 1 2 2 3 3]))

    => [1 2 2 3 3]

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/intersection, pigpen.core/difference
"
  {:arglists '([opts? relations+])
   :added "0.1.0"}
  [& opts-relations]
  (set-operation 'pigpen.set/pig-intersection-multiset opts-relations))

(defn difference
  "Performs a set difference on all relations provided and returns the distinct
results. Optionally takes a map of options as the first parameter.

  Example:

    (pig/difference
      (pig/return [1 2 2 3 3 3 4 5])
      (pig/return [1 2])
      (pig/return [3]))

    => [4 5]

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/difference-multiset, pigpen.core/intersection
"
  {:arglists '([opts? relations+])
   :added "0.1.0"}
  [& opts-relations]
  (set-operation 'pigpen.set/pig-difference opts-relations))

(defn difference-multiset
  "Performs a multiset difference on all relations provided and returns all
results. Optionally takes a map of options as the first parameter.

  Example:

    (pig/difference-multiset
      (pig/return [1 2 2 3 3 3 3 4 5])
      (pig/return [1 2 3])
      (pig/return [1 2 3]))

    => [3 3 4 5]

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/difference, pigpen.core/intersection
"
  {:arglists '([opts? relations+])
   :added "0.1.0"}
  [& opts-relations]
  (set-operation 'pigpen.set/pig-difference-multiset opts-relations))
