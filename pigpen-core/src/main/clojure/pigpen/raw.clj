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

(ns pigpen.raw
  "Contains functions that create basic Pig commands. These are the primitive
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
  "Execute custom code in a script."
  [udf init func args]
  ^:pig {:type :code
         :init init
         :func func
         :udf udf
         :args (vec args)})

;; ********** IO **********

(s/defn load$ :- m/Load$
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
  [location storage opts relation]
  (->
    (command :store relation opts)
    (dissoc :field-type :fields)
    (assoc :location location
           :storage storage
           :description location
           :args (:fields relation))))

(s/defn return$ :- m/Return$
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
  ([alias code]
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
  [projections opts relation]
  (let [{id :id, :as c} (command :project relation opts)]
    (-> c
      (assoc :projections (mapv (partial update-alias-ns id) projections)
             :fields (mapv (partial update-ns id) (mapcat :alias projections))))))

(s/defn bind$* :- m/Bind
  "Used to make a post-bake bind"
  ([func opts relation] (bind$* [] func opts relation))
  ([requires func opts relation]
    (->
      (command :bind (dissoc opts :args :requires :alias :field-type-in :field-type))
      (dissoc :field-type)
      (assoc :ancestors [relation]
             :func func
             :args (vec (get opts :args ['value]))
             :requires (vec (concat requires (:requires opts)))
             :fields [(get opts :alias 'value)]
             :field-type-in (get opts :field-type-in :frozen)
             :field-type (get opts :field-type :frozen)))))

(s/defn bind$ :- m/Bind$
  ([func opts relation] (bind$ [] func opts relation))
  ([requires func opts relation]
    (let [opts' (dissoc opts :args :requires :alias :field-type-in :field-type)
          {id :id, :as c} (command :bind relation opts')]
      (-> c
        (dissoc :field-type)
        (assoc :func func
               :args (vec (get opts :args (get relation :fields)))
               :requires (vec (concat requires (:requires opts)))
               :fields (mapv (partial update-ns+ id) (get opts :alias ['value]))
               :field-type-in (get opts :field-type-in :frozen)
               :field-type (get opts :field-type :frozen))))))

(s/defn sort$ :- m/Sort$
  [key comp opts relation]
  (let [{id :id, :as c} (command :sort relation opts)]
    (-> c
      (assoc :key (update-ns+ (:id relation) key))
      (assoc :comp comp)
      (update-in [:fields] (partial remove #{(update-ns+ id key)})))))

(s/defn rank$ :- m/Rank$
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
  [n opts relation]
  (->
    (command :take relation opts)
    (assoc :n n)))

(s/defn sample$ :- m/Sample$
  [p opts relation]
  (->
    (command :sample relation opts)
    (assoc :p p)))

;; ********** Set **********

(s/defn distinct$ :- m/Distinct$
  [opts relation]
  (command :distinct relation opts))

(s/defn concat$ :- (s/either m/Concat$ m/Op)
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
  [opts relation]
  (->
    (command :reduce relation opts)
    (assoc :fields [(-> relation :fields first)])
    (assoc :arg (-> relation :fields first))))

(defmulti ancestors->fields
  "Get the set of fields from the ancestors"
  (fn [type id ancestors]
    type))

(defmulti fields->keys
  (fn [type fields]
    type))

(s/defn group$ :- m/Group$
  [field-dispatch join-types opts ancestors]
  (let [{id :id, :as c} (command :group ancestors [] opts)
        fields (ancestors->fields field-dispatch id ancestors)]
    (-> c
      (assoc :field-dispatch field-dispatch
             :fields fields
             :keys (fields->keys field-dispatch fields)
             :join-types (vec join-types)))))

(s/defn join$ :- m/Join$
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
  [opts relation]
  (->
    (command :noop relation opts)
    (assoc :args (:fields relation))))

(s/defn store-many$ :- m/StoreMany$
  [outputs]
  ^:pig {:type :store-many
         :id (pigsym "store-many")
         :ancestors (vec outputs)})
