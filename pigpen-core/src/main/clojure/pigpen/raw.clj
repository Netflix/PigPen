;;
;;
;;  Copyright 2013-2015 Netflix, Inc.
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

(ns pigpen.raw
  "Contains functions that create basic commands. These are the primitive
building blocks for more complex operations."
  (:require [pigpen.model :as m]
            [schema.core :as s]))

(set! *warn-on-reflection* true)

(defn pigsym
  "Wraps gensym to facilitate easier mocking"
  [prefix-string]
  (gensym prefix-string))

(defn update-ns [ns sym]
  (symbol (name ns) (name sym)))

(defn update-ns+ [ns sym]
  (if (or (namespace sym) (nil? ns))
    sym
    (update-ns ns sym)))

;; **********

(s/defn ^:private command
  ([type opts]
    ^:pig {:type type
           :id (pigsym (name type))
           :description (:description opts)
           :field-type (get opts :field-type :frozen)
           :opts (-> opts
                   (assoc :type (-> type name (str "-opts") keyword))
                   (dissoc :description :field-type))})
  ([type relation opts]
    {:pre [(map? relation)]}
    (let [{id :id, :as c} (command type opts)]
      (assoc c
             :ancestors [relation]
             :fields (mapv (partial update-ns id) (:fields relation)))))
  ([type ancestors fields opts]
    {:pre [(sequential? ancestors) (sequential? fields)]}
    (let [{id :id, :as c} (command type opts)]
      (assoc c
             :ancestors (vec ancestors)
             :fields (mapv (partial update-ns+ id) fields)))))

;; ********** Util **********

(s/defn code$ :- m/CodeExpr
  "Encapsulation for user code. Used with projection-func$ and project$. You
probably want bind$ instead of this.

The parameter `udf` should be one of:

  :seq    - returns zero or more values
  :fold   - apply a fold aggregation

The parameter `init` is code to be executed once before the user code, `func` is
the user code to execute, and `args` specifies which fields should be passed to
`func`. `args` can also contain strings, which are passed through as constants
to the user code. The result of `func` should be in the same format as bind$.

  Example:

    (code$ :seq '(require my-ns.core) '(fn [args] ...) ['c 'd])

  See also: pigpen.core.op/project$, pigpen.core.op/projection-func$
"
  {:added "0.3.0"}
  [udf init func args]
  ^:pig {:type :code
         :init init
         :func func
         :udf udf
         :args (vec args)})

;; ********** IO **********

(s/defn load$ :- m/Load$
  "Load the data specified by `location`, a string. The parameter `storage` is a
keyword such as :string, :parquet, or :avro that specifies the type of storage
to use. Each platform is responsible for dispatching on storage as appropriate.
The parameters `fields` and `opts` specify what fields this will produce and any
options to the command.

  Example:

    (load$ \"input.tsv\" :string '[value] {})

  See also: pigpen.core.op/store$
"
  {:added "0.3.0"}
  [location storage fields opts]
  (let [id (pigsym "load")]
    ^:pig {:type :load
           :id id
           :description location
           :location location
           :fields (mapv (partial update-ns+ id) fields)
           :field-type (get opts :field-type :native)
           :storage storage
           :opts (assoc opts :type :load-opts)}))

(s/defn store$ :- m/Store$
  "Store the data specified by `location`, a string. The parameter `storage` is
a keyword such as :string, :parquet, or :avro that specifies the type of storage
to use. Each platform is responsible for dispatching on storage as appropriate.
The parameter `opts` specify any options to the command. This command can only
be passed to store-many$ commands or to platform generation commands.

  Example:

    (store$ \"output.tsv\" :string {} relation)

  See also: pigpen.core.op/load$
"
  {:added "0.3.0"}
  [location storage opts relation]
  (->
    (command :store relation opts)
    (dissoc :field-type :fields)
    (assoc :location location
           :storage storage
           :description location
           :args (:fields relation))))

(s/defn store-many$ :- m/StoreMany$
  "Combines multiple store$ commands into a single command. This command can
only be passed to other store-many$ commands or to platform generation commands.

  Example:

    (store-many$ [(store$ \"output1.tsv\" :string {} relation1)
                  (store$ \"output2.tsv\" :string {} relation2)])

  See also: pigpen.core.op/store$
"
  [outputs]
  ^:pig {:type :store-many
         :id (pigsym "store-many")
         :ancestors (vec outputs)})

(s/defn return$ :- m/Return$
  "Return the data as a PigPen relation. The parameter `fields` specifies what
fields the data will contain.

  Example:

    (return$ ['value] [{'value 42} {'value 37}])

  See also: pigpen.core.op/load$
"
  {:added "0.3.0"}
  [fields data]
  (let [id (pigsym "return")]
    ^:pig {:type :return
           :id id
           :field-type :frozen
           :fields (mapv (partial update-ns+ id) fields)
           :data (for [m data]
                   (->> m
                     (map (fn [[f v]] [(update-ns+ id f) v]))
                     (into {})))}))

;; ********** Map **********

(defn projection-field$
  "Project a single field into another, optionally providing an alias for the
new field. If an alias is not specified, the input field name is used. If the
field represents a collection, specify `flatten` as true to flatten the values
of the field into individual records.

  Examples:

    (projection-field$ 'a)         ;; copy the field a as a
    (projection-field$ 'a 'b)      ;; copy the field a as b
    (projection-field$ 'a 'b true) ;; copy the field a as b and flatten a

  See also: pigpen.core.op/project$, pigpen.core.op/projection-func$
"
  {:added "0.3.0"}
  ([field]
    (projection-field$ field [(symbol (name field))] false))
  ([field alias]
    (projection-field$ field alias false))
  ([field alias flatten]
    ^:pig {:type :projection
           :expr {:type :field
                  :field field}
           :flatten flatten
           :alias alias}))

(s/defn projection-func$
  "Apply code to a set of fields, optionally flattening the result. See code$
for details regarding how to express user code.

  Examples:

    (projection-func$ 'a (code$ ...))      ;; scalar result
    (projection-func$ 'a (code$ ...) true) ;; flatten the result collection

  See also: pigpen.core.op/code$, pigpen.core.op/project$,
            pigpen.core.op/projection-field$
"
  {:added "0.3.0"}
  ([alias code :- m/CodeExpr]
    (projection-func$ alias true code))
  ([alias flatten code :- m/CodeExpr]
    ^:pig {:type :projection
           :expr code
           :flatten flatten
           :alias alias}))

(defn ^:private update-alias-ns [id projection]
  (update-in projection [:alias] (partial mapv (partial update-ns id))))

(s/defn project$* :- m/Project
  "Used to make a post-bake project"
  [projections opts relation]
  (let [{id :id, :as c} (command :project opts)]
    (-> c
      (assoc :projections (mapv (partial update-alias-ns id) projections)
             :ancestors [relation]
             :fields (mapv (partial update-ns id) (mapcat :alias projections))))))

(s/defn project$ :- m/Project$
  "Used to manipulate the fields of a relation, either by aliasing them or
applying functions to them. Usually you want bind$ instead of project$, as
PigPen will compile many of the former into one of the latter.

  Example:

    (project$
      [(projection-field$ 'a 'b)
       (projection-func$ 'e
         (code$ :seq
                '(require my-ns.core)
                '(fn [args] ...)
                ['c 'd]))]
      {}
      relation)

In the example above, we apply two operations to the input relation. First, we
alias the input field 'a as 'b. Second, we apply the user code specified by
code$ to the fields 'c and 'd to produce the field 'e. This implies that the
input relation has three fields, 'a, 'c, and 'd, and that the output fields of
this relation are 'b and 'e. If multiple projections are provided that flatten
results, the cross product of those is returned.

  See also: pigpen.core.op/projection-field$, pigpen.core.op/projection-func$,
            pigpen.core.op/code$
"
  {:added "0.3.0"}
  [projections opts relation]
  (let [{id :id, :as c} (command :project relation opts)]
    (-> c
      (assoc :projections (mapv (partial update-alias-ns id) projections)
             :fields (mapv (partial update-ns id) (mapcat :alias projections))))))

(s/defn bind$ :- m/Bind$
  "The way to apply user code to a relation. `func` should be a function that
takes a collection of arguments, and returns a collection of result tuples.
Optionally takes a collection of namespaces to require before executing user
code.

  Example:

    (bind$
      (fn [args]
        ;; do stuff to args, return a value like this:
        [[foo-value bar-value]   ;; result 1
         [foo-value bar-value]   ;; result 2
         ...
         [foo-value bar-value]]) ;; result N
      {:args  '[x y]
       :alias '[foo bar]}
      relation)

  In this example, our function takes `args` which is a tuple of argument values
from the previous relation. Here, this selects the fields x and y. The function
then returns 0-to-many result tuples. Each of those tuples maps to the fields
specified by the alias option. If not specified, args defaults to the fields of
the input relation and alias defaults to a single field `value`. All field names
should be symbols.

  There are many provided bind helper functions, such as map->bind, that take a
normal map function of one arg to one result, and convert it to a bind function.

    (bind$
      (map->bind (fn [x] (* x x)))
      {}
      data)

  See also: pigpen.core.fn/map->bind, pigpen.core.fn/mapcat->bind,
            pigpen.core.fn/filter->bind, pigpen.core.fn/process->bind,
            pigpen.core.fn/key-selector->bind,
            pigpen.core.fn/keyword-field-selector->bind,
            pigpen.core.fn/indexed-field-selector->bind
"
  {:added "0.3.0"}
  ([func opts relation] (bind$ [] func opts relation))
  ([requires func opts relation]
    (let [opts' (dissoc opts :args :requires :alias :types :field-type-in :field-type)
          {id :id, :as c} (command :bind relation opts')]
      (-> c
        (dissoc :field-type)
        (assoc :func func
               :args (vec
                       (or (->>
                             (get opts :args)
                             (map (fn [a]
                                    (if (string? a)
                                      a
                                      (update-ns+ (:id relation) a))))
                             seq)
                           (get relation :fields)))
               :requires (vec (concat requires (:requires opts)))
               :fields (mapv (partial update-ns+ id) (get opts :alias ['value]))
               :field-type-in (get opts :field-type-in :frozen)
               :field-type (get opts :field-type :frozen)
               :types (get opts :types))))))

(s/defn sort$ :- m/Sort$
  "Sort the data in relation. The parameter `key` specifies the field that
should be used to sort the data. The sort field should be a native type; not
serialized. `comp` is either :asc or :desc.

  Example:

    (sort$ 'key :asc {} relation)
"
  {:added "0.3.0"}
  [key comp opts relation]
  (let [{id :id, :as c} (command :sort relation opts)]
    (-> c
      (assoc :key (update-ns+ (:id relation) key))
      (assoc :comp comp)
      (update-in [:fields] (partial remove #{(update-ns+ id key)})))))

(s/defn rank$ :- m/Rank$
  "Rank the input relation. Adds a new field ('index), a long, to the fields of
the input relation.

  Example:

    (rank$ {} relation)

  See also: pigpen.core.op/sort$
"
  {:added "0.3.0"}
  [opts relation]
  (let [{id :id, :as c} (command :rank relation opts)]
    (-> c
      (update-in [:fields] (partial cons (update-ns+ id 'index))))))

;; ********** Filter **********

(s/defn filter$* :- m/Filter
  "Used to make a post-bake filter"
  [fields expr opts relation]
  {:pre [(symbol? relation)]}
  (->
    (command :filter opts)
    (assoc :expr expr
           :fields (vec fields)
           :ancestors [relation]
           :field-type :native)))

(s/defn filter$ :- m/Filter$
  [expr opts relation]
  (->
    (command :filter relation opts)
    (assoc :expr expr
           :field-type :native)))

(s/defn take$ :- m/Take$
  "Returns the first n records from relation.

  Example:

    (take$ 100 {} relation)

  See also: pigpen.core.op/sample$
"
  {:added "0.3.0"}
  [n opts relation]
  (->
    (command :take relation opts)
    (assoc :n n)))

(s/defn sample$ :- m/Sample$
  "Samples the input relation at percentage p, where (<= 0.0 p 1.0).

  Example:

    (sample$ 0.5 {} relation)

  See also: pigpen.core.op/take$
"
  {:added "0.3.0"}
  [p opts relation]
  (->
    (command :sample relation opts)
    (assoc :p p)))

;; ********** Set **********

(s/defn distinct$ :- m/Distinct$
  "Returns the distinct values in relation.

  Example:

    (distinct$ {} relation)

  See also: pigpen.core.op/concat$
"
  {:added "0.3.0"}
  [opts relation]
  (command :distinct relation opts))

(s/defn concat$ :- (s/either m/Concat$ m/Op)
  "Concatenates the set of ancestor relations together. The fields produced by
the concat operation are the fields of the first relation.

  Example:

    (concat$ {} [relation1 relation2])

  See also: pigpen.core.op/distinct$
"
  {:added "0.3.0"}
  [opts ancestors]
  (if-not (next ancestors)
    (first ancestors)
    (command :concat
             ancestors
             (->> ancestors
               first
               :fields
               (map (comp symbol name)))
             opts)))

;; ********** Join **********

(s/defn reduce$ :- m/Reduce$
  "Reduce the entire relation into a single recrod that is the collection of all
records.

  Example:

    (reduce$ {} relation)

  See also: pigpen.core.op/group$
"
  {:added "0.3.0"}
  [opts relation]
  (->
    (command :reduce relation opts)
    (assoc :fields [(-> relation :fields first)])
    (assoc :arg (-> relation :fields first))))

(defmulti ancestors->fields
  "Get the set of fields produced by the ancestors and this command."
  (fn [type id ancestors]
    type))

(defmulti fields->keys
  "Determine which fields are the keys to be used for grouping/joining."
  (fn [type fields]
    type))

(s/defn group$ :- m/Group$
  "Performs a cogroup on the ancestors provided. The parameter `field-dispatch`
should be one of the following, and produces the following output fields:

  :group - [group r0/key r0/value ... rN/key rN/value]
  :join  - [r0/key r0/value ... rN/key rN/value]
  :set   - [r0/value ... rN/value]

The parameter `join-types` is a vector of keywords (:required or :optional)
specifying if each relation is required or optional. The length of join-types
must match the number of relations passed.

  Example:

    (group$
      :group
      [:required :optional]
      {}
      [relation1 relation2])

In this example, the operation performs a cogroup on relation1 and relation2.
The `:group` field-dispatch means that both of those relations will provide a
field with fields `key` and `value`, and the operation will add a `group` field.
The first relation is marked as required and the second is optional.

  See also: pigpen.core.op/join$
"
  {:added "0.3.0"}
  [field-dispatch join-types opts ancestors]
  (let [{id :id, :as c} (command :group ancestors [] opts)
        fields (ancestors->fields field-dispatch id ancestors)]
    (-> c
      (assoc :field-dispatch field-dispatch
             :fields fields
             :keys (fields->keys field-dispatch fields)
             :join-types (vec join-types)))))

(s/defn join$ :- m/Join$
  "Performs a join on the ancestors provided. The parameter `field-dispatch`
should be one of the following, and produces the following output fields:

  :group - [group r0/key r0/value ... rN/key rN/value]
  :join  - [r0/key r0/value ... rN/key rN/value]
  :set   - [r0/value ... rN/value]

The parameter `join-types` is a vector of keywords (:required or :optional)
specifying if each relation is required or optional. The length of join-types
must match the number of relations passed.

  Example:

    (join$
      :join
      [:required :optional]
      {}
      [relation1 relation2])

In this example, the operation performs a join on relation1 and relation2.
The `:join` field-dispatch means that both of those relations will provide a
field with fields `key` and `value`. The first relation is marked as required
and the second is optional.

  See also: pigpen.core.op/group$
"
  {:added "0.3.0"}
  [field-dispatch join-types opts ancestors]
  {:pre [(< 1 (count ancestors))
         (or (every? #{:required} join-types) (= 2 (count ancestors)))
         (sequential? ancestors)
         (sequential? join-types)
         (= (count ancestors) (count join-types))]}
  (let [{id :id, :as c} (command :join ancestors [] opts)
        fields (ancestors->fields field-dispatch nil ancestors)]
    (-> c
      (assoc :field-dispatch field-dispatch
             :fields fields
             :keys (fields->keys field-dispatch fields)
             :join-types (vec join-types)))))

;; ********** Script **********

(s/defn noop$ :- m/NoOp$
  "A no-op command. This is used to introduce a unique id for a command.

  Example:

    (noop$ {} relation)
"
  {:added "0.3.0"}
  [opts relation]
  (->
    (command :noop relation opts)
    (assoc :args (:fields relation))))
