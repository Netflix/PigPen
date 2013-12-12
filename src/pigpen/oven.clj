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
  (:refer-clojure :exclude [ancestors])
  (:require [pigpen.raw :as raw]
            [pigpen.code :as code])
  (:import [org.apache.pig.data DataBag]))

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
    (map raw/tree->command)
    (reverse)))

(defn ^:private extract-references
  "Extract all references from commands and create new reference commands at
   the head of the list."
  [commands]
  (concat 
    (->> commands
      (mapcat raw/command->references)
      (distinct)
      (map raw/register$))
    commands))

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
  (map (fn [c] (raw/update-fields c mapping)) commands))

(defn ^:private dedupe
  "Collapses duplicate commands in a graph. The strategy is to take the set of
   distinct commands, find the first two that can be merged, and merge them to
   produce graph'. Rinse & repeat until there are no more duplicate commands."
  [commands]
  (let [distinct-commands (distinct commands)
        next-merge (next-match distinct-commands)]
    (if next-merge
      (recur (merge-command distinct-commands next-merge))
      distinct-commands)))

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

(defn ^:private command->fat
  "Returns the fields that could be pruned from the specified command. If
   pruning is not possible, returns nil."
  [commands command]
  (let [potential (->> commands
                    (filter #((set (:ancestors %)) (:id command)))
                    (map raw/command->required-fields))]
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
            (let [new-command (raw/remove-fields command fat)]
              (swap! command-set #(-> % (disj command) (conj new-command)))
              new-command))
          command)))))

(defn ^:private command->debug
  "Adds an extra store statement after the command. Returns nil if no debug is
   available"
  [command location]
  (when-let [field-type (or (:field-type command) (:field-type-out command))]
    (-> command
      (raw/bind$ `(pigpen.pig/map->bind pigpen.pig/debug)
                 {:args (:fields command), :field-type-in field-type, :field-type-out :native})
      ;; TODO Fix the location of store commands to match generates instead of binds
      (raw/store$ (str location (:id command)) raw/default-storage {}))))

(defn ^:private debug
  "Creates a debug version of a script. This adds a store command after every command."
  [script location]
  (->> script
    (ancestors)
    (map #(command->debug % location))
    (filter identity)
    (cons script)
    (raw/script$)))

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
  (let [first-relation   (-> commands first :ancestors first)
        first-args       (-> commands first :args)
        first-field-type (-> commands first :field-type-in)
        last-field       (-> commands last :fields first)
        last-field-type  (-> commands last :field-type-out)
        
        requires (->> commands
                   (mapcat :requires)
                   (cons 'pigpen.pig)
                   (distinct)
                   (map (fn [r] `'[~r]))
                   (cons 'clojure.core/require))
        
        func `(pigpen.pig/exec-multi ~first-field-type ~last-field-type
                                     ~(mapv :func commands))
        
        projection (raw/projection-flat$ last-field
                     (raw/code$ DataBag first-args
                       (raw/expr$ requires func)))
        
        description (->> commands (map :description) (clojure.string/join))]
  
    (raw/generate$* first-relation [projection] {:field-type last-field-type
                                                 :description description})))

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

(defn ^:private clean
  "Some optimizations produce unused commands. This prunes them from the graph."
  [commands]
  (let [register-commands (filter #(-> % :type (= :register)) commands)
        referenced-commands (->> commands
                              (mapcat :ancestors) ;; Get all referenced commands
                              (concat register-commands) ;; Add register commands
                              (cons (-> commands last :id)) ;; Add the last command
                              set)]
    (if (= (count commands) (count referenced-commands))
      commands
      (recur (filter #(-> % :id referenced-commands) commands)))))

(defn bake
  "Takes a script as a tree of commands and returns a sequence of commands as a
   directed acyclical graph. This also applies transforms over the data such as
   extracting references as their own commands and deduping equivalent commands"
  [script opts]
  {:pre [(map? script) (map? opts)]}
  (cond-> script
    (:debug opts) (debug (:debug opts))
    true braise
    true merge-order-rank
    true extract-references
    (not= false (:dedupe opts)) dedupe
    (not= false (:prune opts)) trim-fat
    true expand-load-filters
    true optimize-binds
    true clean))
