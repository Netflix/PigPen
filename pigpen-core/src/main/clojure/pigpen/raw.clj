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
building blocks for more complex operations.")

(set! *warn-on-reflection* true)

(defn pigsym
  "Wraps gensym to facilitate easier mocking"
  [prefix-string]
  (gensym prefix-string))

(defn ^:private field?
  "Determines if a symbol is a valid pig identifier"
  [id]
  (boolean
    (and (symbol? id)
         (namespace id)
         (re-find #"^[a-zA-Z][a-zA-Z0-9_]*$" (namespace id))
         (re-find #"^[a-zA-Z][a-zA-Z0-9_]*$" (name id)))))

(defn update-ns [ns sym]
  (symbol (name ns) (name sym)))

(defn update-ns+ [ns sym]
  (if (namespace sym)
    sym
    (update-ns ns sym)))

;; **********

(defn ^:private command
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

(defn expr$
  "Code to be passed to the UDF"
  [init func]
  {:pre [func]}
  ^:pig {:init init
         :func func})

(defn code$
  "Execute custom code in a script."
  [udf args expr]
  {:pre [expr
         (keyword? udf)
         (sequential? args)
         (every? (some-fn field? string?) args)]}
  ^:pig {:type :code
         :expr expr
         :udf udf
         :args (vec args)})

;; ********** IO **********

(defn load$
  [location fields storage opts]
  {:pre [(string? location)
         (sequential? fields)
         (keyword? storage)
         (or (nil? opts) (map? opts))]}
  (let [id (pigsym "load")]
    ^:pig {:type :load
           :id id
           :description location
           :location location
           :fields (mapv (partial update-ns+ id) fields)
           :field-type (get opts :field-type :native)
           :storage storage
           :opts (assoc opts :type :load-opts)}))

(defn store$
  [relation location storage opts]
  {:pre [(string? location)
         (keyword? storage)
         (or (nil? opts) (map? opts))]}
  (->
    (command :store relation opts)
    (dissoc :field-type)
    (assoc :location location
           :storage storage
           :description location)))

(defn return$
  [data fields]
  {:pre [(sequential? data)
         (every? map? data)
         (sequential? fields)
         (every? symbol? fields)]}
  (let [id (pigsym "return")]
    ^:pig {:type :return
           :id id
           :fields (mapv (partial update-ns+ id) fields)
           :data (for [m data]
                   (->> m
                     (map (fn [[f v]] [(update-ns+ id f) v]))
                     (into {})))}))

;; ********** Map **********

(defn projection-field$
  ([field] (projection-field$ field [(symbol (name field))]))
  ([field alias]
    {:pre [(field? field)
           (vector? alias)]}
    ^:pig {:type :projection-field
           :field field
           :alias alias}))

(defn projection-func$
  [alias code]
  {:pre [(map? code)
         (vector? alias)]}
  ^:pig {:type :projection-func
         :code code
         :alias alias})

(defn projection-flat$
  [alias code]
  {:pre [(map? code)
         (vector? alias)]}
  ^:pig {:type :projection-flat
         :code code
         :alias alias})

(defn generate$*
  "Used to make a post-bake generate"
  [relation projections opts]
  {:pre [(symbol? relation)
         (sequential? projections)
         (not-empty projections)]}
  (let [{id :id, :as c} (command :generate opts)]
    (-> c
      (assoc :projections (vec projections)
             :ancestors [relation]
             :fields (mapv (partial update-ns id) (mapcat :alias projections))))))

(defn generate$
  [relation projections opts]
  {:pre [(map? relation)
         (sequential? projections)
         (not-empty projections)]}
  (let [{id :id, :as c} (command :generate relation opts)]
    (-> c
      (assoc :projections (vec projections)
             :fields (mapv (partial update-ns id) (mapcat :alias projections))))))

(defn bind$*
  "Used to make a post-bake bind"
  ([relation func opts] (bind$* relation [] func opts))
  ([relation requires func opts]
    {:pre [func]}
    (->
      (command :bind (dissoc opts :args :requires :alias :field-type-in :field-type-out))
      (dissoc :field-type)
      (assoc :ancestors [relation]
             :func func
             :args (vec (get opts :args ['value]))
             :requires (vec (concat requires (:requires opts)))
             :fields [(get opts :alias 'value)]
             :field-type-in (get opts :field-type-in :frozen)
             :field-type-out (get opts :field-type-out :frozen)))))

(defn bind$
  ([relation func opts] (bind$ relation [] func opts))
  ([relation requires func opts]
    {:pre [func]}
    (let [opts' (dissoc opts :args :requires :alias :field-type-in :field-type-out)
          {id :id, :as c} (command :bind relation opts')]
      (-> c
        (dissoc :field-type)
        (assoc :func func
               :args (vec (get opts :args (get relation :fields)))
               :requires (vec (concat requires (:requires opts)))
               :fields (mapv (partial update-ns+ id) (get opts :alias ['value]))
               :field-type-in (get opts :field-type-in :frozen)
               :field-type-out (get opts :field-type-out :frozen))))))

(defn order$
  [relation key comp opts]
  (->
    (command :order relation opts)
    (assoc :key (update-ns+ (:id relation) key))
    (assoc :comp comp)))

(defn rank$
  [relation opts]
  (let [{id :id, :as c} (command :rank relation opts)]
    (-> c
      (update-in [:fields] (partial cons (update-ns+ id 'index))))))

;; ********** Filter **********

(defn filter$*
  "Used to make a post-bake filter"
  [relation fields expr opts]
  {:pre [(symbol? relation)]}
  (->
    (command :filter opts)
    (assoc :expr expr
           :fields (vec fields)
           :ancestors [relation]
           :field-type :native)))

(defn filter$
  [relation expr opts]
  (->
    (command :filter relation opts)
    (assoc :expr expr
           :field-type :native)))

(defn limit$
  [relation n opts]
  {:pre [((every-pred number? pos?) n)]}
  (->
    (command :limit relation opts)
    (assoc :n n)))

(defn sample$
  [relation p opts]
  {:pre [(float? p) (<= 0.0 p 1.0)]}
  (->
    (command :sample relation opts)
    (assoc :p p)))

;; ********** Set **********

(defn distinct$
  [relation opts]
  (command :distinct relation opts))

(defn union$
  [ancestors opts]
  (if-not (next ancestors)
    (first ancestors)
    (command :union ancestors (->> ancestors first :fields (map (comp symbol name))) opts)))

;; ********** Join **********

(defn reduce$
  [relation opts]
  (command :reduce relation opts))

(defn group$
  [ancestors keys join-types opts]
  {:pre [(sequential? ancestors)
         (sequential? keys)
         (sequential? join-types)
         (= (count ancestors) (count keys) (count join-types))]}
  (let [fields (mapcat :fields ancestors)
        {id :id, :as c} (command :group ancestors fields opts)]
    (->
      c
      (update-in [:fields] (partial cons (update-ns+ id 'group)))
      (assoc :keys (vec keys)
             :join-types (vec join-types)))))

(defn join$
  [ancestors keys join-types opts]
  {:pre [(< 1 (count ancestors))
         (or (every? #{:required} join-types) (= 2 (count ancestors)))
         (sequential? ancestors)
         (sequential? keys)
         (sequential? join-types)
         (= (count ancestors) (count keys) (count join-types))]}
  (let [fields (mapcat :fields ancestors)]
    (->
      (command :join ancestors fields opts)
      (assoc :keys (vec keys)
             :join-types (vec join-types)))))

;; ********** Script **********

(defn script$
  [outputs]
  {:pre [(sequential? outputs)]}
  ^:pig {:type :script
         :id (pigsym "script")
         :ancestors (vec outputs)})
