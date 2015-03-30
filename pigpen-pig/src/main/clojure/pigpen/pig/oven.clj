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

(ns pigpen.pig.oven
  (:require [pigpen.raw :as raw]
            [pigpen.pig.raw :as pig-raw]
            [pigpen.oven]))

(defmulti command->references :type)
(defmethod command->references :default [_] nil)
(defmulti storage->references identity)
(defmethod storage->references :default [_] nil)

(defmethod command->references :load [{:keys [storage]}]
  (storage->references storage))

(defmethod command->references :store [{:keys [storage]}]
  (storage->references storage))

(defn ^:private command->options
  "Gets any options required for a command"
  [command]
  (-> command :opts :pig-options))

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

(defn ^:private extract-references
  "Extract all references from commands and create new reference commands at
   the head of the list."
  [{:keys [extract-references?]} commands]
  (when extract-references?
    (extract-* command->references pig-raw/register$ commands)))

(defn ^:private extract-options
  "Extract all options from commands and create new option commands at
   the head of the list."
  [{:keys [extract-options?]} commands]
  (when extract-options?
    (extract-* command->options (fn [[o v]] (pig-raw/option$ o v)) commands)))

(defn ^:private add-pigpen-jar
  [{:keys [add-pigpen-jar? pigpen-jar-location]} commands]
  (when add-pigpen-jar?
    (cons
      (pig-raw/register$ pigpen-jar-location)
      commands)))

;; **********

(defn ^:private next-sort-rank
  "Finds a pig/sort or pig/sort-by followed by a pig/map-indexed."
  [commands lookup]
  (->> commands
    ;; Look for rank commands
    (filter (comp #{:rank} :type))
    ;; Find the first that's after an sort & has no sort of its own
    (some (fn [rank]
            ;; rank will only ever have a single ancestor
            (let [sort (-> rank :ancestors first lookup)]
              (when (-> sort :type #{:sort})
                ;; Return both the rank & sort commands
                [rank sort]))))))

(defn ^:private merge-sort-rank
  "Looks for a pig/sort or pig/sort-by followed by a pig/map-indexed. Moves the
   sort operation into the rank command."
  [_ commands]
  ;; Build an id > command lookup
  (let [lookup (->> commands (map (juxt :id identity)) (into {}))]
    ;; Try to find the next potential rank command.
    (if-let [[next-rank {:keys [key comp ancestors]}] (next-sort-rank commands lookup)]
      ;; If we find one, update the rank & recur
      (recur
         _
        (for [command commands]
          (if-not (= command next-rank)
            command
            ;; When we find the rank command, add the sort-keys to it and update it
            ;; to point at the sort's project command.
            (assoc command
                   :key key
                   :comp comp
                   :ancestors ancestors))))
      ;; If we don't find one, we're done
      commands)))

;; **********

(defn ^:private expand-load-filters
  "Load commands can specify a native filter. This filter must be defined with
   the load command because of some nuances in Pig. This expands that into an
   actual command."
  [_ commands]
  ;; TODO possibly make expansion available to all commands?
  (->> commands
    (mapcat (fn [{:keys [type id opts fields] :as c}]
              (if (and (= type :load) (:filter opts))
                (let [filter (:filter opts)
                      id' (symbol (str id "_0"))]
                  [(assoc c :id id')
                   (->
                     (raw/filter$* fields filter {} id')
                     (assoc :id id))])
                [c])))))

;; **********

(defn ^:private dec-rank
  "Pig starts rank at 1. This decrements every rank to match clojure."
  [_ commands]
  (->> commands
    (mapcat (fn [{:keys [type id opts fields] :as c}]
              (if (= type :rank)
                (let [id-str (str id "_0")
                      id' (symbol id-str)]
                  [(assoc c :id id')
                   {:type :bind
                    :id id
                    :ancestors [id']
                    :fields [(symbol (str id) "$0") (symbol (str id) "value")]
                    :field-type-in :frozen
                    :field-type :frozen
                    :func '(pigpen.runtime/process->bind
                             (fn [[i v]]
                               [(dec i) v]))
                    :args [(symbol id-str "$0") (symbol id-str "value")]
                    :requires []
                    :types nil}])
                [c])))))

;; **********

(defn ^:private split-project
  "Splits every project command into two so that column pruning works"
  [_ commands]
  (->> commands
    (mapcat (fn [{:keys [type id fields field-type projections opts] :as c}]
              (if (= type :project)
                (let [id' (symbol (str id "_0"))
                      projections-a (map-indexed
                                      (fn [i p]
                                        (-> p
                                          (assoc :flatten false)
                                          (assoc :alias [(symbol (name id') (str "value" i))])
                                          (dissoc :types)))
                                      projections)
                      projections-b (map-indexed
                                      (fn [i p]
                                        (-> p
                                          (assoc :expr {:type :field
                                                        :field (symbol (name id') (str "value" i))})
                                          (update-in [:alias] (partial mapv (partial raw/update-ns id)))))
                                      projections)]
                  [(-> c
                     (assoc :id id')
                     (assoc :projections (vec projections-a))
                     (assoc :fields (mapcat :alias projections-a)))
                   (->
                     (raw/project$* projections-b {} id')
                     (assoc :id id)
                     (assoc :projections projections-b)
                     (assoc :fields (mapcat :alias projections-b)))])
                [c])))))

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
  {:added "0.3.0"}
  ([query] (bake {} query))
  ([opts query]
    (pigpen.oven/bake
      :pig
      {extract-options     1.1
       extract-references  1.2
       add-pigpen-jar      1.3
       merge-sort-rank     1.4
       expand-load-filters 2.1
       dec-rank            2.2
       split-project       4.5}
      (merge {:extract-references? true
              :extract-options?    true
              :add-pigpen-jar?     true
              :pigpen-jar-location "pigpen.jar"}
             opts)
      query)))
