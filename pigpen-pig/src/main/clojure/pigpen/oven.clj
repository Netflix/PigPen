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

(ns pigpen.oven
  "Contains functions used to convert an expression tree into an expression
graph. This is required for local execution or script generation. Applies a
number of optimizations and transforms to the graph.
"
  (:refer-clojure :exclude [ancestors])
  (:require [clojure.set]
            [pigpen.raw :as raw]
            [pigpen.code :as code])
  (:import [org.apache.pig.data DataBag]))

(set! *warn-on-reflection* true)

(defmulti ^:private tree->command
  "Converts a tree node into a single edge. This is done by converting the
   reference to another node to that node's id"
  :type)

(defmethod tree->command :default
  [command]
  (update-in command [:ancestors] #(mapv :id %)))

(defn ^:private update-field
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
    (let [projections' (vec
                         (for [{:keys [type] :as p} projections]
                           (case type
                             (:projection-func :projection-flat)
                             (update-in p [:code :args]
                                        (fn [args] (mapv #(update-field % id-mapping) args)))
                             
                             :projection-field
                             (update-in p [:field] #(update-field % id-mapping)))))]
      (assoc command :projections projections'))))

(defn ^:private update-fields
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

(defmulti ^:private command->required-fields
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

(defmulti ^:private remove-fields
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

(defn ^:private extract-*
  "Extract something from commands and create new commands at
   the head of the list."
  [extract create commands]
  {:pre [(ifn? extract) (ifn? create) (sequential? commands)]}
  (concat
    (->> commands
      (mapcat extract)
      (distinct)
      (map create))
    commands))

(defn ^:private command->references
  "Gets any references required for a command"
  [jar-location command]
  (case (:type command)
    :code [jar-location]
    :bind [jar-location]
    :storage (:references command)
    (:load :store) (command->references jar-location (:storage command))
    (:projection-func filter) (command->references jar-location (:code command))
    :generate (->> command :projections (mapcat (partial command->references jar-location)))
    nil))

(defn ^:private command->options
  "Gets any options required for a command"
  [command]
  (-> command :opts :pig-options))

(defn ^:private extract-references
  "Extract all references from commands and create new reference commands at
   the head of the list."
  [jar-location commands]
  (extract-* (partial command->references jar-location) raw/register$ commands))

(defn ^:private extract-options
  "Extract all options from commands and create new option commands at
   the head of the list."
  [commands]
  (extract-* command->options (fn [[o v]] (raw/option$ o v)) commands))

;; **********

(defn ^:private ancestors
  "Gets all ancestors for a command"
  [command]
  (tree-seq :ancestors :ancestors command))

(defn ^:private braise
  "Converts a script tree into a graph of the operation nodes. Pass in any node
   and this returns the list of that node and its ancestors, in the order
   they'll need to be executed. This also calls tree->edge on each node, such
   that it will now refer to the id of the node instead of the actual node."
  [command]
  (->> command
    (ancestors)
    (map tree->command)
    (reverse)))

;; **********

(defn ^:private next-match
  "Find the first two commands that are equivalent and return them as a map.
   If no two are the same, returns nil."
  ;; Start with an empty list of commands
  ([commands] (next-match {} commands))
  ;; Pull the first command & compare it against what we've seen so far
  ([cache [command & rest]]
    (let [id (:id command)
          ;; Commands are equivalent iff they are identical except for their ids
          abstract-command (dissoc command :id)
          ;; Cache contains all of the commands we've seen so far (sans ids),
          ;; so we check if this command is present
          existing-command (cache abstract-command)]
      (cond
        ;; If we've seen this command, map it to the one we saw before
        existing-command {id (:id existing-command)}
        ;; Otherwise add this command to what we've seen & continue
        rest (recur (assoc cache abstract-command command) rest)))))

(defn ^:private merge-command
  "Applies a mapping of command ids to a set of commands. This updates the id
   and ancestor keys of each command map. There will remain duplicates in the
   result - this just updates the id space."
  [commands mapping]
  (map (fn [c] (update-fields c mapping)) commands))

(defn ^:private dedupe
  "Collapses duplicate commands in a graph. The strategy is to take the set of
   distinct commands, find the first two that can be merged, and merge them to
   produce graph'. Rinse & repeat until there are no more duplicate commands."
  [commands]
  (let [distinct-commands (vec (distinct commands))
        next-merge (next-match distinct-commands)]
    (if next-merge
      (recur (merge-command distinct-commands next-merge))
      distinct-commands)))

;; **********

(defn ^:private next-order-rank
  "Finds a pig/sort or pig/sort-by followed by a pig/map-indexed."
  [commands lookup]
  (->> commands
    ;; Look for rank commands
    (filter #(= (:type %) :rank))
    ;; Find the first that's after an order & has no sort of its own
    (some (fn [c]
            ;; rank will only ever have a single ancestor
            (let [a (-> c :ancestors first lookup)]
              (when (and (= (:sort-keys c) [])
                         (= (:type a) :order))
                ;; Return both the rank & order commands
                [c a]))))))

(defn ^:private merge-order-rank
  "Looks for a pig/sort or pig/sort-by followed by a pig/map-indexed. Moves the
   order operation into the rank command."
  [commands]
  ;; Build an id > command lookup
  (let [lookup (->> commands (map (juxt :id identity)) (into {}))]
    ;; Try to find the next potential rank command.
    (if-let [[next-rank {:keys [sort-keys ancestors]}] (next-order-rank commands lookup)]
      ;; If we find one, update the rank & recur
      (recur
        (for [command commands]
          (if-not (= command next-rank)
            command
            ;; When we find the rank command, add the sort-keys to it and update it
            ;; to point at the sort's generate command.
            (assoc command
                   :sort-keys sort-keys
                   :ancestors ancestors))))
      ;; If we don't find one, we're done
      commands)))

;; **********

(defn ^:private command->fat
  "Returns the fields that could be pruned from the specified command. If
   pruning is not possible, returns nil."
  [commands command]
  (let [potential (->> commands
                    (filter #((set (:ancestors %)) (:id command)))
                    (map command->required-fields))]
    (if (and (not-empty potential) (every? (comp not nil?) potential))
      (clojure.set/difference (set (:fields command)) (set (mapcat identity potential))))))

(defn ^:private trim-fat
  "Removes any fields that are produced by a relation that are not used by any children."
  [commands]
  (let [command-set (atom (set commands))]
    (reverse
      (for [command (reverse commands)]
        (if-let [fat (command->fat @command-set command)]
          (if-not (seq fat) command
            (let [new-command (remove-fields command fat)]
              (swap! command-set #(-> % (disj command) (conj new-command)))
              new-command))
          command)))))

;; **********

(defn ^:private command->debug
  "Adds an extra store statement after the command. Returns nil if no debug is
   available"
  [command location]
  (when-let [field-type (or (:field-type command) (:field-type-out command))]
    (when-not (get-in command [:opts :implicit-schema])
      (-> command
        (raw/bind$ [] `(pigpen.runtime/map->bind pigpen.pig/debug)
                   {:args (:fields command), :field-type-in field-type, :field-type-out :native})
        ;; TODO Fix the location of store commands to match generates instead of binds
        (raw/store$ (str location (:id command)) raw/default-storage {})))))

(defn ^:private debug
  "Creates a debug version of a script. This adds a store command after every command."
  [script location]
  (->> script
    (ancestors)
    (map #(command->debug % location))
    (filter identity)
    (cons script)
    (raw/script$)))

;; **********

;; TODO Add documentation
(defn ^:private find-bind-sequence [commands]
  (let [terminals (->> commands
                    (mapcat (fn [{:keys [id ancestors]}] (for [a ancestors] [a id])))
                    (group-by first)
                    (filter (fn [[k v]] (> (count v) 1)))
                    (map key)
                    (into #{}))]
    (reduce (fn [[before binds after] {:keys [id type ancestors] :as command}]
              (if (and (= type :bind)
                       (or (empty? binds)
                           (and (not (terminals (:id (last binds))))
                                (= ancestors [(:id (last binds))]))))
                [before (conj binds command) after]
                (if (empty? binds)
                  [(conj before command) binds after]
                  [before binds (conj after command)])))
            [[] [] []] commands)))

(defn ^:private bind->generate [commands]
  ;; TODO make sure all inner field types are :frozen
  (let [first-relation   (-> commands first :ancestors first)
        first-args       (-> commands first :args)
        first-field-type (-> commands first :field-type-in)
        last-field       (-> commands last :fields first)
        last-field-type  (-> commands last :field-type-out)
        implicit-schema  (some (comp :implicit-schema :opts) commands)
        
        requires (code/build-requires (mapcat :requires commands))
        
        func `(pigpen.pig/exec
                [(pigpen.pig/pre-process ~first-field-type)
                 ~@(mapv :func commands)
                 (pigpen.pig/post-process ~last-field-type)])
        
        projection (raw/projection-flat$ last-field
                     (raw/code$ DataBag first-args
                       (raw/expr$ requires func)))
        
        description (->> commands (map :description) (clojure.string/join))]
  
    (raw/generate$* first-relation [projection] {:field-type last-field-type
                                                 :description description
                                                 :implicit-schema implicit-schema})))

(defn ^:private optimize-binds [commands]
  (if (= 1 (count commands))
    commands
    (let [[before binds after] (find-bind-sequence commands)]
      (if (empty? binds)
        commands
        (let [generate (bind->generate binds)
              next (concat before [generate] after)
              next (merge-command next {(-> binds last :id) (:id generate)})]
          (recur next))))))

;; **********

(defn ^:private expand-load-filters
  "Load commands can specify a native filter. This filter must be defined with
   the load command because of some nuances in Pig. This expands that into an
   actual command."
  [commands]
  ;; TODO possibly make expansion available to all commands?
  (->> commands
    (mapcat (fn [{:keys [type id opts fields] :as c}]
              (if (and (= type :load) (:filter opts))
                (let [filter (:filter opts)
                      id' (symbol (str id "_0"))]
                  [(assoc c :id id')
                   (-> id'
                     (raw/filter-native$* fields filter {})
                     (assoc :id id))])
                [c])))))

;; **********

(defn ^:private make-join-fields
  ;; TODO combine with the logic in pigpen.raw
  ;; Can't use update-fields because of the dupes
  "Create the fields for new join/cogroup commands"
  [type ancestors]
  (case type
    :join (->> ancestors
            (mapcat (fn [{id :id}] [[[id 'key]] [[id 'value]]]))
            vec)
    :group (->> ancestors
             (mapcat (fn [{id :id}] [[[id] 'key] [[id] 'value]]))
             (cons 'group)
             vec)))

(defn ^:private alias-self-join
  "Creates new ids for key-selectors for a join or cogroup"
  [command-lookup {:keys [ancestors type] :as join}]
  ;; For each ancestor of a join, duplicate it with a new id
  (let [ancestors' (vec
                     (for [id ancestors]
                       (let [ancestor (command-lookup id)
                             id' (raw/pigsym (name (:type ancestor)))]
                         (assoc ancestor :id id'))))
        ;; Create a new join with the new ids
        join' (-> join
                (assoc :ancestors (mapv :id ancestors'))
                (assoc :fields (make-join-fields type ancestors')))]
    ;; Return both the id->id' mapping and the new commands
    [(zipmap ancestors (map :id ancestors'))
     (concat ancestors' [join'])]))

(defn ^:private alias-self-joins
  "Self-joins create ambiguous fields and are not supported by Pig. This changes
the alias of each key-selector so that they are unique."
  [commands]
  (let [command-lookup (->> commands (map (juxt :id identity)) (into {}))
        id-map (atom {})]
    (->> commands
      (mapcat (fn [{:keys [type ancestors] :as command}]
                ;; update only joins and cogroups
                (if (and (type #{:join :group})
                         (not= (count ancestors) (count (set ancestors))))
                  (let [[id-map' commands'] (alias-self-join command-lookup command)]
                    (swap! id-map merge id-map')
                    commands')
                  ;; any joins will exist before their consumers, so we just
                  ;; update id-map as we find them
                  [(update-fields command @id-map)])))
      vec)))

;; **********

(defn ^:private clean
  "Some optimizations produce unused commands. This prunes them from the graph."
  [commands]
  (let [referenced-commands (->> commands
                              (mapcat :ancestors) ;; Get all referenced commands
                              (cons (-> commands last :id)) ;; Add the last command
                              set)
        command-valid? (fn [{:keys [id type]}]
                         (or (type #{:register :option})
                             (referenced-commands id)))]
    (if (every? command-valid? commands)
      commands
      (recur (filter command-valid? commands)))))

;; **********

(defn bake
  "Takes a query as a tree of commands and returns a sequence of commands as a
directed acyclical graph. Also applies optimizations to the query such as
deduping. This command is idempotent and can be called many times. The options
are only honored the first time called.

This is useful for generating multiple outputs with the same ids. Call bake once
to create a graph with the final ids and then pass that to any command that
produces a non-pigpen output.

  Example:
	  (let [command (->>
	                  (pig/load-tsv \"foo.tsv\")
	                  (pig/map inc)
	                  (pig/filter even?))
	        graph (oven/bake command)]
	    (pig/show graph)
	    (pig/generate-script graph))

  See also: pigpen.core/generate-script, pigpen.core/write-script,
            pigpen.core/dump, pigpen.core/show
"
  {:added "0.2.5"} ; since 0.1.0, but exposed 0.2.5
  ([query] (bake {} query))
  ([opts query]
    {:pre [(->> query meta keys (some #{:pig :baked})) (map? opts)]}
    (let [jar-location (or (:pigpen-jar-location opts) "pigpen.jar")
          extract-references (partial extract-references jar-location)]
      (if (-> query meta :baked)
        query
        (cond-> query
          (:debug opts) (debug (:debug opts)) ;; TODO add a debug-lite version
          true braise
          true merge-order-rank
          true extract-options
          true extract-references
          (not= false (:dedupe opts)) dedupe
          (not= false (:prune opts)) trim-fat
          true expand-load-filters
          true optimize-binds
          true alias-self-joins
          true clean
          true (with-meta {:baked true}))))))
