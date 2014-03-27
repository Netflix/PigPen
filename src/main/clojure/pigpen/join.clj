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
  (:require [pigpen.extensions.core :refer [pp-str forcat]]
            [pigpen.pig :as pig]
            [pigpen.raw :as raw]
            [pigpen.code :as code])
  (:import [org.apache.pig.data DataByteArray]))

(set! *warn-on-reflection* true)

(defn ^:private select->generate
  "Performs the key selection prior to a join. If join-nils is true, we leave
nils as frozen nils so they appear as values. Otherwise we return a nil value as
nil and let the join take its course. If sentinel-nil is true, nil keys are
coerced to ::nil so they can be differentiated from outer joins later."
  ;; TODO - If this is an inner join, we can filter nil keys before the join
  [{:keys [join-nils sentinel-nil]} {:keys [from key-selector on by]}]
  (let [key-selector (or key-selector on by 'identity)]
    (-> from
      (raw/bind$ (if sentinel-nil
                   `(pigpen.pig/key-selector->bind (comp pig/sentinel-nil ~key-selector))
                   `(pigpen.pig/key-selector->bind ~key-selector))
                 {:field-type-out (if join-nils :frozen :frozen-with-nils)
                  :implicit-schema true})
      (raw/generate$ [(raw/projection-field$ 0 'key)
                      (raw/projection-field$ 1 'value)] {}))))

(defn fold-fn*
  "See pigpen.core/fold-fn"
  [pre combinef reducef post]
  {:pre [pre combinef reducef post]}
  (code/assert-arity* pre 1)
  (code/assert-arity* combinef 0)
  (code/assert-arity* combinef 2)
  (code/assert-arity* reducef 2)
  (code/assert-arity* post 1)
  {:type :fold
   :pre pre
   :combinef combinef
   :reducef reducef
   :post post})

(defn ^:private projection-fold [fold field alias]
  (if fold
    (raw/projection-func$ alias (raw/code$ "Algebraic" [field] (raw/expr$ "" fold)))
    (raw/projection-field$ field alias)))

(defn group*
  "See pigpen.core/group-by, pigpen.core/cogroup"
  [selects f opts]
  (let [relations  (mapv (partial select->generate opts) selects)
        keys       (for [r relations] ['key])
        values     (cons 'group (for [r relations] [[(:id r)] 'value]))
        folds      (mapv projection-fold (cons nil (map :fold selects)) values (map #(symbol (str "value" %)) (range)))
        join-types (mapv #(get % :type :optional) selects)]
    (code/assert-arity f (count values))
    (-> relations
      (raw/group$ keys join-types (dissoc opts :fold))
      (raw/generate$ folds {})
      (raw/bind$ `(pigpen.pig/map->bind ~f) {:args (mapv :alias folds)}))))

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

(defn fold*
  "See pigpen.core/fold"
  [relation fold opts]
  (let [keys       [raw/group-all$]
        values     [[[(:id relation)] 'value]]
        join-types [:optional]]
    (-> [relation]
      (raw/group$ keys join-types opts)
      (raw/generate$ [(projection-fold fold (first values) 'value)] {}))))

(defn join*
  "See pigpen.core/join"
  [selects f {:keys [all-args] :as opts}]
  (let [relations  (mapv (partial select->generate opts) selects)
        keys       (for [r relations] ['key])
        values     (forcat [r relations]
                     (if all-args
                       [[[(:id r) 'key]] [[(:id r) 'value]]]
                       [[[(:id r) 'value]]]))
        join-types (mapv #(get % :type :required) selects)]
    (code/assert-arity f (count values))
    (-> relations
      (raw/join$ keys join-types opts)
      (raw/bind$ `(pigpen.pig/map->bind ~f) {:args values}))))

(defmacro group-by
  "Groups relation by the result of calling (key-selector item) for each item.
This produces a sequence of map entry values, similar to using seq with a
map. Each value will be a lazy sequence of the values that match key.
Optionally takes a map of options, including :parallel and :fold.

  Example:

    (pig/group-by :a foo)
    (pig/group-by count {:parallel 20} foo)

  Options:

    :parallel - The degree of parallelism to use

  See also: pigpen.core/cogroup

  See pigpen.fold for more info on :fold options.
"
  ([key-selector relation] `(group-by ~key-selector {} ~relation))
  ([key-selector opts relation]
    `(group* [(merge
                {:from ~relation
                 :key-selector (code/trap ~key-selector)
                 :type :optional}
                ~(code/trap-values #{:on :by :key-selector :fold} opts))]
             '(fn [~'k ~'v] (clojure.lang.MapEntry. ~'k ~'v))
             (assoc ~opts :description ~(pp-str key-selector)))))

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
        Use pig/fold to do a parallel reduce.

  See also: pigpen.core/fold, pigpen.core/into
"
  ([f relation]
    `(group-all* ~relation
                 (code/trap (partial clojure.core/reduce ~f))
                 {:description ~(pp-str f)}))
  ([f val relation]
    `(group-all* ~relation
                 (code/trap (partial clojure.core/reduce ~f ~val))
                 {:description ~(pp-str f)})))

(defmacro fold
  "Computes a parallel reduce of the relation. This is done in multiple stages
using reducef and combinef. First, combinef is called with no args to produce a
seed value. Then, reducef reduces portions of the data using that seed value.
Finally, combinef is used to reduce each of the intermediate values. If combinef
is not specified, reducef is used for both. Fold functions defined using
pigpen.fold/fold-fn can also be used.

  Example:

    (pig/fold + foo)
    (pig/fold + (fn [acc _] (inc acc)) foo)
    (pig/fold (fold/fold-fn + (fn [acc _] (inc acc))) foo)

  See pigpen.fold for more info on fold functions.
"
  ([reducef relation]
    `(if (-> ~reducef :type #{:fold})
       (fold* ~relation
              (code/trap ~reducef)
              {})
       (fold ~reducef ~reducef ~relation)))
  ([combinef reducef relation]
    `(fold* ~relation
            (code/trap (fold-fn* identity ~combinef ~reducef identity))
            {})))

(defmacro cogroup
  "Joins many relations together by a common key. Each relation specifies a
key-selector function on which to join. A combiner function is applied to each
join key and all values from each relation that match that join key. This is
similar to join, without flattening the data. Optionally takes a map of options.

  Example:

    (pig/cogroup [(foo :on :a)
                  (bar :on :b, :type :required, :fold (fold/count))]
                 (fn [key foos bar-count] ...)
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
collections. The last argument is an optional map of options. A fold function
can be specified to aggregate groupings in parallel. See pigpen.fold for more
info on fold functions.

  Options:

    :parallel - The degree of parallelism to use
    :join-nils - Whether nil keys from each relation should be treated as equal

  See also: pigpen.core/join, pigpen.core/group-by
"
  ([selects f] `(cogroup ~selects ~f {}))
  ([selects f opts]
    (let [selects# (->> selects
                     (map (partial cons :from))
                     (map (partial code/trap-values #{:on :by :key-selector :fold}))
                     vec)]
      `(group* ~selects#
               (code/trap ~f)
               (assoc ~opts :description ~(pp-str f))))))

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
    :join-nils - Whether nil keys from each relation should be treated as equal

  See also: pigpen.core/cogroup, pigpen.core/union
"
  ([selects f] `(join ~selects ~f {}))
  ([selects f opts]
    (let [selects# (->> selects
                     (map (partial cons :from))
                     (map (partial code/trap-values #{:on :by :key-selector}))
                     vec)]
      `(join* ~selects#
              (code/trap ~f)
              (assoc ~opts :description ~(pp-str f))))))

(defmacro filter-by
  "Filters a relation by the keys in another relation. The key-selector function
is applied to each element of relation. If the resulting key is present in keys,
the value is kept. Otherwise it is dropped. nils are dropped or preserved based
on whether there is a nil value present in keys. This operation is referred to
as a semi-join in relational databases.

  Example:

    (let [keys (pig/return [1 3 5])
          data (pig/return [{:k 1, :v \"a\"}
                            {:k 2, :v \"b\"}
                            {:k 3, :v \"c\"}
                            {:k 4, :v \"d\"}
                            {:k 5, :v \"e\"}])]
      (pig/filter-by :k keys data))

    => (pig/dump *1)
    [{:k 1, :v \"a\"}
     {:k 3, :v \"c\"}
     {:k 5, :v \"e\"}]

  Options:

    :parallel - The degree of parallelism to use

  Note: keys must be distinct before this is used or you will get duplicate values.
  Note: Unlike filter, this joins relation with keys and can be potentially expensive.

  See also: pigpen.core/filter, pigpen.core/remove-by, pigpen.core/intersection
"
  ([key-selector keys relation] `(filter-by ~key-selector ~keys {} ~relation))
  ([key-selector keys opts relation]
    `(join* [{:from ~keys :key-selector 'identity}
             {:from ~relation :key-selector (code/trap ~key-selector)}]
            '(fn [~'k ~'v] ~'v)
            (assoc ~opts :description ~(pp-str key-selector)
                         :sentinel-nil true))))

(defmacro remove-by
  "Filters a relation by the keys in another relation. The key-selector function
is applied to each element of relation. If the resulting key is _not_ present in
keys, the value is kept. Otherwise it is dropped. nils are dropped or preserved
based on whether there is a nil value present in keys. This operation is
referred to as an anti-join in relational databases.

  Example:

    (let [keys (pig/return [1 3 5])
          data (pig/return [{:k 1, :v \"a\"}
                            {:k 2, :v \"b\"}
                            {:k 3, :v \"c\"}
                            {:k 4, :v \"d\"}
                            {:k 5, :v \"e\"}])]
      (pig/remove-by :k keys data))

    => (pig/dump *1)
    [{:k 2, :v \"b\"}
     {:k 4, :v \"d\"}]

  Options:

    :parallel - The degree of parallelism to use

  Note: Unlike remove, this joins relation with keys and can be potentially expensive.

  See also: pigpen.core/remove, pigpen.core/filter-by, pigpen.core/difference
"
  ([key-selector keys relation] `(remove-by ~key-selector ~keys {} ~relation))
  ([key-selector keys opts relation]
    (let [f '(fn [[k _ _ v]] (when (nil? k) [v]))]
      `(->
         (join* [{:from ~keys :key-selector 'identity :type :optional}
                 {:from ~relation :key-selector (code/trap ~key-selector)}]
                'vector
                (assoc ~opts :description ~(pp-str key-selector)
                             :all-args true
                             :sentinel-nil true))
         (raw/bind$ '(pig/mapcat->bind ~f) {})))))
