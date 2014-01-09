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
  (boolean (and (symbol? id)
                (re-find #"^[a-zA-Z][a-zA-Z0-9_]*$" (name id)))))

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
    (->
      (command type opts)
      (assoc :ancestors [relation]
             :fields (:fields relation))))
  ([type ancestors fields opts]
    {:pre [(sequential? ancestors) (sequential? fields)]}
    (->
      (command type opts)
      (assoc :ancestors (vec ancestors)
             :fields (vec fields)))))

;; ********** Util **********

;; TODO - raw pig commands

(defn register$
  "A Pig REGISTER command. jar is the qualified location of the jar."
  [jar]
  {:pre [(string? jar)]}
  ^:pig {:type :register
         :jar jar})

(defn option$
  "A Pig option. Takes the name and a value. Not used locally."
  [option value]
  {:pre [(string? option)]}
  ^:pig {:type :option
         :option option
         :value value})

(defn expr$
  "Code to be passed to the UDF"
  [init func]
  {:pre [func]}
  ^:pig {:init init
         :func func})

(defn code$
  "Execute custom code in a script."
  [^Class return args expr]
  {:pre [expr return (sequential? args)]}
  ^:pig {:type :code
         :expr expr
         :return (last (clojure.string/split (.getName return) #"\."))
         :args (vec args)})

;; ********** IO **********

(defn storage$
  "A Pig storage definition. This is not a store command, but rather how to use
   pre-built jars that contain custom storage implementations. This will be
   passed to either a load or store form."
  [references func args]
  {:pre [((every-pred string? not-empty) func)
         (every? string? references)]}
  ^:pig {:type :storage
         :references references
         :func func
         :args args})

(def default-storage
  (storage$ [] "PigStorage" []))

(defn load$
  [location fields storage opts]
  {:pre [(string? location)
         (sequential? fields)
         (= (:type storage) :storage)
         (or (nil? opts) (map? opts))]}
  ^:pig {:type :load
         :id (pigsym "load")
         :description location
         :location location
         :fields fields
         :field-type (get opts :field-type :native)
         :storage storage
         :opts (assoc opts :type :load-opts)})

(defn store$
  [relation location storage opts]
  {:pre [(string? location)
         (= (:type storage) :storage)
         (or (nil? opts) (map? opts))]}
  (->
    (command :store relation opts)
    (dissoc :field-type)
    (assoc :location location
           :storage storage
           :description location)))

(defn return$
  [data]
  {:pre [(sequential? data)
         (every? map? data)
         (every? symbol? (mapcat keys data))]}
  ^:pig {:type :return
         :id (pigsym "return")
         :fields (vec (keys (first data)))
         :data data})

;; ********** Map **********

(defn projection-field$
  ([field] (projection-field$ field field))
  ([field alias]
    {:pre [(field? field)
           (field? alias)]}
    ^:pig {:type :projection-field
           :field field
           :alias alias}))

(defn projection-func$
  [alias code]
  {:pre [(map? code)
         (field? alias)]}
  ^:pig {:type :projection-func
         :code code
         :alias alias})

(defn projection-flat$
  [alias code]
  {:pre [(map? code)
         (field? alias)]}
  ^:pig {:type :projection-flat
         :code code
         :alias alias})

(defn generate$*
  "Used to make a post-bake generate"
  [relation projections opts]
  {:pre [(symbol? relation)
         (sequential? projections)
         (not-empty projections)]}
  (->
    (command :generate opts)
    (assoc :projections (vec projections)
           :ancestors [relation]
           :fields (mapv :alias projections))))

(defn generate$
  [relation projections opts]
  {:pre [(map? relation)
         (sequential? projections)
         (not-empty projections)]}
  (->
    (command :generate relation opts)
    (assoc :projections (vec projections)
           :fields (mapv :alias projections))))

(defn bind$
  [relation func opts]
  {:pre [func]}
  (->
    (command :bind relation (dissoc opts :args :requires :alias :field-type-in :field-type-out))
    (dissoc :field-type)
    (assoc :func func
           :args (vec (get opts :args ['value]))
           :requires (vec (get opts :requires []))
           :fields [(get opts :alias 'value)]
           :field-type-in (get opts :field-type-in :frozen)
           :field-type-out (get opts :field-type-out :frozen))))

(defn order$
  [relation sort-keys opts]
  (->
    (command :order relation opts)
    (update-in [:fields] #(remove #{'key} %))
    (assoc :sort-keys sort-keys)))

(defn rank$
  [relation sort-keys opts]
  (->
    (command :rank relation opts)
    (update-in [:fields] conj '$0)
    (assoc :sort-keys sort-keys)))

;; ********** Filter **********

(defn filter$
  [relation code opts]
  {:pre [(map? code)]}
  (->
    (command :filter relation opts)
    (assoc :code code)))

(defn filter-native$*
  "Used to make a post-bake filter-native"
  [relation fields expr opts]
  {:pre [(symbol? relation)]}
  (->
    (command :filter-native opts)
    (assoc :expr expr
           :fields (vec fields)
           :ancestors [relation]
           :field-type :native)))

(defn filter-native$
  [relation expr opts]
  (->
    (command :filter-native relation opts)
    (assoc :expr expr
           :field-type :native)))

(defn distinct$
  [relation opts]
  (command :distinct relation opts))

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

;; TODO single union -> first relation
(defn union$
  [ancestors opts]
  (command :union ancestors (-> ancestors first :fields) opts))

;; ********** Join **********

(def group-all$ "A special key for group-all" ::group-all)

(defn group$
  [ancestors keys join-types opts]
  {:pre [(sequential? ancestors)
         (sequential? keys)
         (sequential? join-types)
         (= (count ancestors) (count keys) (count join-types))]}
  (let [fields (cons 'group (for [r ancestors, f (:fields r)] [[(:id r)] f]))]
    (->
      (command :group ancestors fields opts)
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
  (let [fields (for [r ancestors, f (:fields r)] [[(:id r) f]])]
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
