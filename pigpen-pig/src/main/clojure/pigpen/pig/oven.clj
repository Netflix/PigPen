(ns pigpen.pig.oven
  (:require [pigpen.pig.raw :as pig-raw]
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
  [commands]
  (extract-* command->references pig-raw/register$ commands))

(defn ^:private extract-options
  "Extract all options from commands and create new option commands at
   the head of the list."
  [commands]
  (extract-* command->options (fn [[o v]] (pig-raw/option$ o v)) commands))

(defn ^:private add-pigpen-jar
  [commands jar-location]
  (cons
    (pig-raw/register$ jar-location)
    commands))

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
    {:pre [(->> query meta keys (some #{:pig :baked})) (map? opts)]}
    (if (-> query meta :baked)
      query
      (as-> query %
        (pigpen.oven/bake :pig opts %)
        (extract-references %)
        (extract-options %)
        (add-pigpen-jar % (or (:pigpen-jar-location opts) "pigpen.jar"))
        (with-meta % {:baked true})))))
