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
  (:require [clojure.pprint])
  (:import [java.io StringWriter]))

(defn pigsym
  "Wraps gensym to facilitate easier mocking"
  [prefix-string]
  (gensym prefix-string))

(defn ^:private field?
  "Determines if a symbol is a valid pig identifier"
  [id]
  (boolean (and (symbol? id)
                (re-find #"^[a-zA-Z][a-zA-Z0-9_]*$" (name id)))))

;; TODO move these functions to oven
(defmulti tree->command
  "Converts a tree node into a single edge. This is done by converting the
   reference to another node to that node's id"
  :type)

(defmethod tree->command :default
  [command]
  (update-in command [:ancestors] #(mapv :id %)))

(defn update-field
  "Updates a single field with an id mapping. This is aware of the
   pigpen field structure:

   foo             > foo
   [[foo bar]]     > foo::bar
   [[foo bar] baz] > foo::bar.baz

"
  [field id-mapping]
  {:pre [field (map? id-mapping)]}
  (if-not (sequential? field) field
    (let [[relation dereference] field
          new-relation (mapv #(get id-mapping % %) relation)]
      (if dereference
        [new-relation dereference]
        [new-relation]))))

(defn ^:private update-projections
  "Updates fields used in projections"
  [{:keys [projections] :as command} id-mapping]
  (if-not projections
    command
    (let [projections' (for [p projections]
                         (if (= (:type p) :projection-func)
                           (update-in p [:code :args]
                                      (fn [args] (mapv #(update-field % id-mapping) args)))
                           p))]
      (assoc command :projections projections'))))

(defn update-fields
  "The default way to update ids in a command. This updates the id of the
   command and any ancestors."
  [command id-mapping]
  {:pre [(map? command) (map? id-mapping)]}  
  (-> command
    (update-in [:id] (fn [id] (id-mapping id id)))
    (update-in [:ancestors] #(mapv (fn [id] (id-mapping id id)) %))
    (update-in [:fields] #(mapv (fn [f] (update-field f id-mapping)) %))
    (update-in [:args] #(mapv (fn [f] (update-field f id-mapping)) %))
    (update-projections id-mapping)))

;; **********

(defmulti command->required-fields
  "Returns the fields required for a command. Always a set."
  :type)

(defmethod command->required-fields :default [command] nil)

(defmethod command->required-fields :projection-field [command]
  #{(:field command)})

(defmethod command->required-fields :projection-func [command]
  (->> command :code :args (filter (some-fn symbol? sequential?)) (set)))

(defmethod command->required-fields :generate [command]
  (->> command :projections (mapcat command->required-fields) (set)))

;; **********

(defmulti remove-fields
  "Prune unnecessary fields from a command. The default does nothing - add an
   override for commands that have prunable fields."
  (fn [command fields] (:type command)))

(defmethod remove-fields :default [command fields] command)

(defmethod remove-fields :generate [command fields]
  {:pre [(map? command) (set? fields)]}
  (-> command
    (update-in [:projections] (fn [ps] (remove (fn [p] (fields (:alias p))) ps)))
    (update-in [:fields] #(remove fields %))))

;; **********

(defn command->references
  "Gets any references required for a command"
  [command]
  (case (:type command)
    :code ["pigpen.jar"]
    :bind ["pigpen.jar"]
    :storage (:references command)
    (:load :store) (command->references (:storage command))
    (:projection-func filter) (command->references (:code command))
    :generate (->> command :projections (mapcat command->references))
    nil))

;; **********

(defn pp-str
 "Pretty prints to a string"
 [object]
 (let [writer (StringWriter.)]
   (clojure.pprint/pprint object writer)
   (.toString writer)))

(defn command->description [{:keys [id]}]
  "Returns a simple human readable description of a command"
  (str id))

(defn command->description+ [{:keys [id description]}]
  "Returns a verbose human readable description of a command"
  (if description
   (str id "\n\n" description)
   (str id)))

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
