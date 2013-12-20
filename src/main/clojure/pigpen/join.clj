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

(ns pigpen.join
  "Commands to join and group data.

  Note: Most of these are present in pigpen.core. Normally you should use those instead.
"
  (:refer-clojure :exclude [group-by into reduce])
  (:require [pigpen.util :as util]
            [pigpen.pig :as pig]
            [pigpen.raw :as raw]
            [pigpen.code :as code])
  (:import [org.apache.pig.data DataByteArray]))

(set! *warn-on-reflection* true)

(defn ^:private select?
  "Returns true if the specified value is a select clause"
  [potential]
  ;; TODO use ^:pig to identify commands
  ;; A select should be in the form (relation on/by fn) or (relation on/by fn required/optional)
  (and (list? potential)
       (case (count potential)
         3 (let [[r c f] potential] ('#{on by} c))
         4 (let [[r c f o] potential] (and ('#{on by} c) ('#{required optional} o)))
         false)))

(defn ^:private split-selects
  "Identifies the function and options in the join and cogroup args"
  [selects]
  (let [[s1 s0] (vec (take-last 2 selects))]
    ;; last position is potentially opts and second to last is not a select,
    ;; making it the fn
    (if (and (map? s0) (not (select? s1)))
      [(drop-last 2 selects) s1 s0]
      ;; second to last is a select - last is fn
      [(drop-last selects) s0 {}])))

(defn ^:private select->generate
  "Performs the key selection prior to a join. If join-nils? is true, we leave nils
   as frozen nils so they appear as values. Otherwise we return a nil value as nil
   and let the join take its course."
  ;; TODO - If this is an inner join, we can filter nil keys before the join
  [join-nils? [relation key-selector]]
  (let [value (first (:fields relation))
        code (raw/code$ DataByteArray [value]
                        (raw/expr$ `(require '[pigpen.pig])
                                   `(pigpen.pig/exec :frozen ~(if join-nils? :frozen :frozen-with-nils) ~key-selector)))
        projections [(raw/projection-func$ 'key code)
                     (raw/projection-field$ 'value)]]
    (raw/generate$ relation projections {})))

(defn group*
  "See pigpen.core/group-by, pigpen.core/cogroup"
  [selects f opts]
  (let [relations  (mapv #(select->generate (:join-nils opts) %) selects)
        keys       (for [r relations] ['key])
        values     (cons 'group (for [r relations] [[(:id r)] 'value]))
        join-types (mapv #(or (nth % 2) :optional) selects)]
    (code/assert-arity f (count values))
    (-> relations
      (raw/group$ keys join-types opts)
      (raw/bind$ `(pigpen.pig/map->bind ~f) {:args values}))))

(defn group-all*
  "See pigpen.core/into, pigpen.core/reduce"
  [relation f opts]
  (code/assert-arity f 2)
  (let [keys       [raw/group-all$]
        values     [[[(:id relation)] 'value]]
        join-types [:optional]]
    (-> [relation]
      (raw/group$ keys join-types opts)
      (raw/bind$ `(pigpen.pig/map->bind ~f) {:args values}))))

(defn join*
  "See pigpen.core/join"
  [selects f opts]
  (let [relations  (mapv #(select->generate (:join-nils opts) %) selects)
        keys       (for [r relations] ['key])
        values     (for [r relations] [[(:id r) 'value]])
        join-types (mapv #(or (nth % 2) :required) selects)]
    (code/assert-arity f (count values))
    (-> relations
      (raw/join$ keys join-types opts)
      (raw/bind$ `(pigpen.pig/map->bind ~f) {:args values}))))

(defmacro group-by
  "Groups relation by the result of calling (key-selector item) for each item.
This produces a sequence of map entry values, similar to using seq with a
map. Each value will be a lazy sequence of the values that match key.
Optionally takes a map of options.

  Example:

    (pig/group-by :a foo)
    (pig/group-by count {:parallel 20} foo)

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/cogroup
"
  ([key-selector relation] `(group-by ~key-selector {} ~relation))
  ([key-selector opts relation]
    `(group* [[~relation (code/trap-locals ~key-selector) :optional]]
             '(fn [~'k ~'v] (clojure.lang.MapEntry. ~'k ~'v))
             (assoc ~opts :description ~(util/pp-str key-selector)))))

(defmacro into
  "Returns a new relation with all values from relation conjoined onto to.

  Note: This operation uses a single reducer and won't work for large datasets.

  See also: pigpen.core/reduce
"
  [to relation]
  `(group-all* ~relation (quote (partial clojure.core/into ~to)) {:description (str "into " ~to)}))

;; TODO If reduce returns a seq, should it be flattened for further processing?
(defmacro reduce
  "Reduce all items in relation into a single value. Follows semantics of
clojure.core/reduce. If a sequence is returned, it is kept as a single value
for further processing.

  Example:

    (pig/reduce + foo)
    (pig/reduce conj [] foo)

  Note: This operation uses a single reducer and won't work for large datasets.

  See also: pigpen.core/into
"
  ([f relation]
    `(group-all* ~relation (code/trap-locals (partial clojure.core/reduce ~f)) {:description ~(util/pp-str f)}))
  ([f val relation]
    `(group-all* ~relation (code/trap-locals (partial clojure.core/reduce ~f ~val)) {:description ~(util/pp-str f)})))

;; TODO trap locals in key selectors

(defmacro cogroup
  "Joins many relations together by a common key. Each relation specifies a
key-selector function on which to join. A combiner function is applied to each
join key and all values from each relation that match that join key. This is
similar to join, without flattening the data. Optionally takes a map of options.

  Example:

    (pig/cogroup (foo on :a)
                 (bar on :b required)
                 (fn [key foos bars] ...)
                 {:parallel 20})

In this example, foo and bar are other pig queries and :a and :b are the
key-selector functions for foo and bar, respectively. These can be any
functions - not just keywords. There can be more than two select clauses.
By default, a matching key value from eatch source relation is optional,
meaning that keys don't have to exist in all source relations to be part of the
output. To specify a relation as required, add 'required' to the select clause.
The third argument is a function used to consolidate matching key values. For
each uniqe key value, this function is called with the value of the key and all
values with that key from foo and bar. As such, foos and bars are both
collections. The last argument is an optional map of options.

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/join, pigpen.core/group-by
"
  {:arglists '([selects+ f opts?])}
  [& selects]
  (let [[selects# f# opts#] (split-selects selects)
        _ (doseq [s# selects#] (assert (select? s#) (str s# " is not a valid select clause. If provided, opts should be a map.")))
        selects# (mapv (fn [[r _ k t]] `[~r '~k ~(keyword t)]) selects#)]
    `(group* ~selects# (code/trap-locals ~f#) (assoc ~opts# :description ~(util/pp-str f#)))))

;; TODO group strategies

(defmacro join
  "Joins many relations together by a common key. Each relation specifies a
key-selector function on which to join. A function is applied to each join
key and each pair of values from each relation that match that join key.
Optionally takes a map of options.

  Example:

    (pig/join (foo on :a)
              (bar on :b optional)
              (fn [f b] ...)
              {:parallel 20})

In this example, foo and bar are other pig queries and :a and :b are the
key-selector functions for foo and bar, respectively. These can be any
functions - not just keywords. There can be more than two select clauses.
By default, a matching key value from eatch source relation is required,
meaning that they must exist in all source relations to be part of the output.
To specify a relation as optional, add 'optional' to the select clause. The
third argument is a function used to consolidate matching key values. For each
uniqe key value, this function is called with each set of values from the cross
product of each source relation. By default, this does a standard inner join.
Use 'optional' to do outer joins. The last argument is an optional map of
options.

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/cogroup, pigpen.core/union
"
  {:arglists '([selects+ f opts?])}
  [& selects]
  (let [[selects# f# opts#] (split-selects selects)
        _ (doseq [s# selects#] (assert (select? s#) (str s# " is not a valid select clause. If provided, opts should be a map.")))
        selects# (mapv (fn [[r _ k t]] `[~r '~k ~(keyword t)]) selects#)]
    `(join* ~selects# (code/trap-locals ~f#) (assoc ~opts# :description ~(util/pp-str f#)))))

;; TODO join strategies
;; TODO semi-join
;; TODO anti-join
