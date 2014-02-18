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

(defn ^:private quote-select-clause
  "Takes a sequence of options for a join or group command, converts them into
a map, and optionally traps specific values. The parameter quotable determines
which ones should be quoted and trapped."
  [quotable select]
  (->> select
    (partition 2)
    (map (fn [[k v]]
           (let [k (keyword k)
                 k (if (#{:on :by} k) :key-selector k)]
             [k (if (quotable k) `(code/trap '~(ns-name *ns*) ~v) v)])))
    (clojure.core/into {})))

(defn ^:private select->generate
  "Performs the key selection prior to a join. If join-nils? is true, we leave nils
   as frozen nils so they appear as values. Otherwise we return a nil value as nil
   and let the join take its course."
  ;; TODO - If this is an inner join, we can filter nil keys before the join
  [join-nils? requires {:keys [from key-selector]}]
  (-> from
    (raw/bind$ requires `(pigpen.pig/key-selector->bind ~key-selector)
               {:field-type-out (if join-nils? :frozen :frozen-with-nils)
                :implicit-schema true})
    (raw/generate$ [(raw/projection-field$ 0 'key)
                    (raw/projection-field$ 1 'value)] {})))

(defn fold? [value]
  (and (-> value meta :pig) (-> value :type #{:fold})))

(defn fold-fn*
  "See pigpen.core/fold-fn"
  [requires combinef reducef finalf]
  (code/assert-arity combinef 0)
  (code/assert-arity combinef 2)
  (code/assert-arity reducef 2)
  (code/assert-arity finalf 1)
  (raw/fold$ (code/build-requires requires)
             `(pig/exec-combinef ~combinef)
             `(pig/exec-reducef ~((eval combinef)) ~reducef)
             `(pig/exec-finalf ~combinef ~finalf)))

(defn ^:private projection-fold [fold field alias]
  {:pre [(or (nil? fold) (fold? fold))]}
  (if fold
    (raw/projection-func$ alias (raw/code$ "Algebraic" [field] fold))
    (raw/projection-field$ field alias)))

(defn group*
  "See pigpen.core/group-by, pigpen.core/cogroup"
  [selects requires f opts]
  (let [relations  (mapv (partial select->generate (:join-nils opts) requires) selects)
        keys       (for [r relations] ['key])
        values     (cons 'group (for [r relations] [[(:id r)] 'value]))
        folds      (mapv projection-fold (cons nil (map :fold selects)) values (repeatedly #(raw/pigsym "field")))
        join-types (mapv #(get % :type :optional) selects)]
    (code/assert-arity f (count values))
    (-> relations
      (raw/group$ keys join-types (dissoc opts :fold))
      (raw/generate$ folds {})
      (raw/bind$ requires `(pigpen.pig/map->bind ~f) {:args (mapv :alias folds)}))))

(defn group-all*
  "See pigpen.core/into, pigpen.core/reduce"
  [relation requires f opts]
  (code/assert-arity f 2)
  (let [keys       [raw/group-all$]
        values     [[[(:id relation)] 'value]]
        join-types [:optional]]
    (-> [relation]
      (raw/group$ keys join-types opts)
      (raw/bind$ requires `(pigpen.pig/map->bind ~f) {:args values}))))

(defn fold*
  "See pigpen.core/fold"
  ([relation requires combinef reducef finalf opts]
    (fold* relation (fold-fn* requires combinef reducef finalf) opts))
  ([relation fold opts]
  (let [keys       [raw/group-all$]
        values     [[[(:id relation)] 'value]]
        join-types [:optional]]
    (-> [relation]
      (raw/group$ keys join-types opts)
      (raw/generate$ [(projection-fold fold (first values) 'value)] {})))))

(defn join*
  "See pigpen.core/join"
  [selects requires f opts]
  (let [relations  (mapv (partial select->generate (:join-nils opts) requires) selects)
        keys       (for [r relations] ['key])
        values     (for [r relations] [[(:id r) 'value]])
        join-types (mapv #(get % :type :required) selects)]
    (code/assert-arity f (count values))
    (-> relations
      (raw/join$ keys join-types opts)
      (raw/bind$ requires `(pigpen.pig/map->bind ~f) {:args values}))))

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
    `(group* [(merge
                {:from ~relation
                  :key-selector (code/trap '~(ns-name *ns*) ~key-selector)
                  :type :optional}
                ~(quote-select-clause #{:on :by :key-selector}
                                      (mapcat identity opts)))]
             ['~(ns-name *ns*)]
             '(fn [~'k ~'v] (clojure.lang.MapEntry. ~'k ~'v))
             (assoc ~opts :description ~(util/pp-str key-selector)))))

(defmacro into
  "Returns a new relation with all values from relation conjoined onto to.

  Note: This operation uses a single reducer and won't work for large datasets.

  See also: pigpen.core/reduce
"
  [to relation]
  `(group-all* ~relation [] (quote (partial clojure.core/into ~to)) {:description (str "into " ~to)}))

;; TODO If reduce returns a seq, should it be flattened for further processing?
(defmacro reduce
  "Reduce all items in relation into a single value. Follows semantics of
clojure.core/reduce. If a sequence is returned, it is kept as a single value
for further processing.

  Example:

    (pig/reduce + foo)
    (pig/reduce conj [] foo)

  Note: This operation uses a single reducer and won't work for large datasets.

  See also: pigpen.core/fold, pigpen.core/into
"
  ([f relation]
    `(group-all* ~relation ['~(ns-name *ns*)] (code/trap '~(ns-name *ns*) (partial clojure.core/reduce ~f)) {:description ~(util/pp-str f)}))
  ([f val relation]
    `(group-all* ~relation ['~(ns-name *ns*)] (code/trap '~(ns-name *ns*) (partial clojure.core/reduce ~f ~val)) {:description ~(util/pp-str f)})))

(defmacro fold-fn
  "Creates a pre-defined fold operation. Can be used with cogroup and group-by
to aggregate large groupings in parallel. See pigpen.core/fold for usage of
reducef and combinef.

  Example:

    (def count
      (pig/fold-fn + (fn [acc _] (inc acc))))

    (def sum
      (pig/fold-fn +))

    (defn sum-by [f]
      (pig/fold-fn + (fn [acc value] (+ acc (f value)))))
"
  ([reducef] `(fold-fn ~reducef ~reducef identity))
  ([combinef reducef] `(fold-fn ~combinef ~reducef identity))
  ([combinef reducef finalf]
    `(fold-fn*
       ['~(ns-name *ns*)]
       (code/trap '~(ns-name *ns*) ~combinef)
       (code/trap '~(ns-name *ns*) ~reducef)
       (code/trap '~(ns-name *ns*) ~finalf))))

(defmacro fold
  "Computes a parallel reduce of the relation. This is done in multiple stages
using reducef and combinef. First, combinef is called with no args to produce a
seed value. Then, reducef reduces portions of the data using that seed value.
Finally, combinef is used to reduce each of the intermediate values. If combinef
is not specified, reducef is used for both. Fold functions defined using fold-fn
can also be used.

  Example:

    (pig/fold + foo)
    (pig/fold + (fn [acc _] (inc acc)) foo)
    (pig/fold (pig/fold-fn + (fn [acc _] (inc acc))) foo)
"
  ([reducef relation]
    `(if (fold? ~reducef)
       (fold* ~relation ~reducef {})
       (fold ~reducef ~reducef ~relation)))
  ([combinef reducef relation]
    `(fold* ~relation
            ['~(ns-name *ns*)]
            (code/trap '~(ns-name *ns*) ~combinef)
            (code/trap '~(ns-name *ns*) ~reducef)
            'identity
            {})))

(defmacro cogroup
  "Joins many relations together by a common key. Each relation specifies a
key-selector function on which to join. A combiner function is applied to each
join key and all values from each relation that match that join key. This is
similar to join, without flattening the data. Optionally takes a map of options.

  Example:

    (pig/cogroup [(foo :on :a)
                  (bar :on :b :type :required)]
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
  ([selects f] `(cogroup ~selects ~f {}))
  ([selects f opts]
    (let [selects# (mapv #(quote-select-clause #{:on :by :key-selector} (cons :from %)) selects)]
      `(group* ~selects# ['~(ns-name *ns*)] (code/trap '~(ns-name *ns*) ~f) (assoc ~opts :description ~(util/pp-str f))))))

(defmacro join
  "Joins many relations together by a common key. Each relation specifies a
key-selector function on which to join. A function is applied to each join
key and each pair of values from each relation that match that join key.
Optionally takes a map of options.

  Example:

    (pig/join [(foo :on :a)
               (bar :on :b :type :optional)]
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
  ([selects f] `(join ~selects ~f {}))
  ([selects f opts]
    (let [selects# (mapv #(quote-select-clause #{:on :by :key-selector} (cons :from %)) selects)]
      `(join* ~selects# ['~(ns-name *ns*)] (code/trap '~(ns-name *ns*) ~f) (assoc ~opts :description ~(util/pp-str f))))))

;; TODO semi-join
;; TODO anti-join
