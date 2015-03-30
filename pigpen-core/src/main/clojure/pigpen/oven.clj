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

(ns pigpen.oven
  "Contains functions used to convert an expression tree into an expression
graph. This is required for local execution or script generation. Applies a
number of optimizations and transforms to the graph.
"
  (:refer-clojure :exclude [ancestors])
  (:require [clojure.set]
            [pigpen.raw :as raw]
            [pigpen.code :as code]))

(set! *warn-on-reflection* true)

(defn ^:private update-if [m ks f & args]
  (if (get-in m ks)
    (apply update-in m ks f args)
    m))

(defmulti ^:private tree->command
  "Converts a tree node into a single edge. This is done by converting the
   reference to another node to that node's id"
  :type)

(defmethod tree->command :default
  [command]
  (update-if command [:ancestors] #(mapv :id %)))

(defn ^:private update-field
  "Updates a single field with an id mapping"
  [id-mapping field]
  {:pre [field ((some-fn map? fn?) id-mapping)]}
  (cond
    (string? field)
    field

    (and (symbol? field)
         (namespace field))
    (let [r (symbol (namespace field))
          r' (id-mapping r r)]
      (symbol (name r') (name field)))

    :else
    (throw (ex-info "Invalid field" {:field field}))))

(defn ^:private update-command-fields
  "Updates command-specific fields"
  [command update-fn]
  (case (:type command)
    :project       (update-in command [:projections] (partial mapv #(update-command-fields % update-fn)))
    :projection    (-> command
                     (update-in [:expr] #(update-command-fields % update-fn))
                     (update-in [:alias] (partial mapv update-fn)))
    :code          (update-in command [:args] (partial mapv update-fn))
    :field         (update-in command [:field] update-fn)

    :bind          (update-in command [:args] (partial mapv update-fn))
    :store         (update-in command [:args] (partial mapv update-fn))
    :sort          (update-in command [:key] update-fn)
    :reduce        (update-in command [:arg] update-fn)
    (:group :join) (update-in command [:keys] (partial mapv update-fn))
    :noop          (update-in command [:args] (partial mapv update-fn))
    command))

(defn ^:private update-fields
  "The way to update ids in a command. This updates the field names in a
   command."
  [command id-mapping]
  {:pre [(map? command) ((some-fn map? fn?) id-mapping)]}
  (let [update-fn (partial update-field id-mapping)]
    (-> command
      (update-if [:fields] (partial mapv update-fn))
      (update-command-fields update-fn))))

(defn ^:private update-ids
  "The way to update ids in a command. This updates the id of the
   command and any ancestors, along with the field names."
  [command id-mapping]
  {:pre [(map? command) ((some-fn map? fn?) id-mapping)]}
  (let [update-fn (partial update-field id-mapping)]
    (-> command
      (update-if [:id] (fn [id] (id-mapping id id)))
      (update-if [:ancestors] (partial mapv (fn [id] (id-mapping id id))))
      (update-fields id-mapping))))

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
  [_ command]
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
          ;; Commands are equivalent iff they are identical except for their ids and field namespaces
          abstract-command (-> command
                             (dissoc :id)
                             (update-fields (constantly "")))
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
  [mapping commands]
  (map (fn [c] (update-ids c mapping)) commands))

(defn ^:private dedupe
  "Collapses duplicate commands in a graph. The strategy is to take the set of
   distinct commands, find the first two that can be merged, and merge them to
   produce graph'. Rinse & repeat until there are no more duplicate commands."
  [_ commands]
  (let [distinct-commands (vec (distinct commands))
        next-merge (next-match distinct-commands)]
    (if next-merge
      (recur _ (merge-command next-merge distinct-commands))
      distinct-commands)))

;; **********

(defn ^:private command->debug
  "Adds an extra store statement after the command. Returns nil if no debug is
   available"
  [location command]
  (when-let [field-type (or (:field-type command) (:field-type command))]
    (->> command
      (raw/bind$ [] `(pigpen.runtime/map->bind pigpen.runtime/debug)
                 {:args (:fields command), :field-type-in field-type, :field-type :native})
      ;; TODO Fix the location of store commands to match projects instead of binds
      (raw/store$ (str location (:id command)) :string {}))))

;; TODO add a debug-lite version
(defn ^:private debug
  "Creates a debug version of a script. This adds a store command after every command."
  [{:keys [debug]} command]
  (when debug
    (->> command
      (ancestors)
      (map (partial command->debug debug))
      (filter identity)
      (cons command)
      (raw/store-many$))))

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

(defn ^:private bind->project [commands platform]
  ;; TODO make sure all inner field types are :frozen
  (let [first-relation   (-> commands first :ancestors first)
        first-args       (-> commands first :args)
        first-field-type (-> commands first :field-type-in)
        last-field       (-> commands last :fields)
        last-field-type  (-> commands last :field-type)
        last-types       (-> commands last :types)

        requires (code/build-requires (mapcat :requires commands))

        func `(comp
                (pigpen.runtime/process->bind (pigpen.runtime/pre-process ~platform ~first-field-type))
                ~@(mapv :func commands)
                (pigpen.runtime/process->bind (pigpen.runtime/post-process ~platform ~last-field-type)))

        projection (raw/projection-func$
                     (mapv (comp symbol name) last-field)
                     (raw/code$ :seq
                                requires
                                func
                                first-args))

        description (->> commands (map :description) (clojure.string/join))]

    (raw/project$*
      [(assoc projection :types last-types)]
      {:field-type last-field-type
       :description description}
      first-relation)))

(defn ^:private optimize-binds
  [{:keys [platform] :as opts} commands]
  (if (= 1 (count commands))
    commands
    (let [[before binds after] (find-bind-sequence commands)]
      (if (empty? binds)
        commands
        (let [project (bind->project binds platform)
              next (concat before [project] after)
              next (merge-command {(-> binds last :id) (:id project)} next)]
          (recur opts next))))))

;; **********

(defn ^:private alias-self-join
  "Creates new ids for key-selectors for a join or cogroup"
  [command-lookup {:keys [type id ancestors field-dispatch] :as join}]
  ;; For each ancestor of a join, create a no-op with a new id
  (let [ancestors' (->> ancestors
                     (mapv #(->> %
                              command-lookup
                              (raw/noop$ {})
                              tree->command)))
        ancestor-ids' (mapv :id ancestors')
        fields' (raw/ancestors->fields field-dispatch id ancestors')
        ;; Create a new join with the new ids
        join' (-> join
                (assoc :ancestors ancestor-ids')
                (assoc :fields fields')
                (assoc :keys (raw/fields->keys field-dispatch fields')))]
    ;; Return both the id->id' mapping and the new commands
    [{(first ancestors) (cycle ancestor-ids')}
     (conj ancestors' join')]))

(defn ^:private alias-self-joins
  "Self-joins create ambiguous fields. This introduces a no-op for each
   key-selector so that they are unique."
  [_ commands]
  (let [command-lookup (->> commands
                         (map (juxt :id identity))
                         (into {}))
        ;; Because a self join corrects by changing a->a1 and a->a2, we
        ;; alternate between a1 and a2 when performing the lookup. This atom
        ;; maintains that state. This works because each subsequent command has
        ;; only one reference to the join.
        lookup-indexes (atom {})]

    (loop [[{:keys [type ancestors] :as command} & more] commands
           id-map {} ;; keep a history of all ids we've changed
           result []]

      (if-not command
        result

        ;; update only joins and cogroups that have ambiguous ancestors
        (if (and (type #{:join :group})
                 (not= (count ancestors) (count (set ancestors))))

          ;; create the new commands
          (let [[id-map' commands'] (alias-self-join command-lookup command)]
            (recur more (merge id-map id-map') (vec (concat result commands'))))

          ;; any joins will exist before their consumers, so we just
          ;; update with id-map as we find them
          (recur more id-map
                 (conj result
                       (update-ids command
                                   (fn [id else]
                                     (if-let [aliases (get id-map id)]
                                       (let [n (get (swap! lookup-indexes #(update-in % [id] (fnil inc -1))) id)]
                                         (->> aliases (drop n) first))
                                       else))))))))))

;; **********

(defn ^:private clean
  "Some optimizations produce unused commands. This prunes them from the graph."
  [_ commands]
  (let [referenced-commands (->> commands
                              (mapcat :ancestors) ;; Get all referenced commands
                              (cons (-> commands last :id)) ;; Add the last command
                              set)
        command-valid? (fn [{:keys [id type]}]
                         (or (type #{:register :option})
                             (referenced-commands id)))]
    (if (every? command-valid? commands)
      commands
      (recur _ (filter command-valid? commands)))))

;; **********

(defn mark-baked [_ commands]
  (with-meta commands {:baked true}))

(defn default-operations []
  {debug               0
   braise              1
   dedupe              2
   optimize-binds      3
   alias-self-joins    4
   clean               5
   mark-baked          6})

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

  [platform operations opts query]
  {:pre [(map? opts)]}
  (assert (->> query meta keys (some #{:pig :baked})) "Query was not a pigpen query")
  (if (-> query meta :baked)
    query
    (->>
      (merge (default-operations) operations)
      (sort-by second)
      (reduce (fn [commands [op _]]
                (if-let [commands' (op (assoc opts :platform platform) commands)]
                  commands'
                  commands))
              query))))
