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

(ns pigpen.pig.script
  "Contains functions for converting an expression graph into a Pig script

Nothing in here will be used directly with normal PigPen usage.
See pigpen.core and pigpen.pig
"
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [join replace]]
            [schema.core :as s]
            [pigpen.model :as m]
            [pigpen.extensions.core :refer [zip]]
            [pigpen.raw :as raw]
            [pigpen.pig.runtime]))

(set! *warn-on-reflection* true)

(defn ^:private escape+quote [f]
  (str "'" (when f
             (-> f
               (clojure.string/replace "\\" "\\\\")
               (clojure.string/replace "'" "\\'"))) "'"))

(defn ^:private escape-id [id]
  (clojure.string/replace id "-" "_"))

(defn ^:private format-field
  ([field] (format-field nil field))
  ([context field]
    (cond
      (string? field) (escape+quote field)
      (symbol? field) (case context
                        (:group :reduce)
                        (if (= (name field) "group")
                          (name field)
                          (str (namespace field) "." (name field)))

                        :join
                        (str (namespace field) "::" (name field))

                        (name field)))))

(defn ^:private type->pig-type [type]
  (case type
    :int     "int"
    :long    "long"
    :float   "float"
    :double  "double"
    :string  "chararray"
    :boolean "boolean"
    :binary  "bytearray"))

(def ^:private clj->op
 {"and" " AND "
  "or" " OR "
  "=" " == "
  "not=" " != "
  "<" " < "
  ">" " > "
  "<=" " <= "
  ">=" " >= "})

(defn ^:private expr->script
 ([expr] (expr->script {} expr))
 ([scope expr]
   (cond
     (nil? expr)    nil
     (number? expr) (cond
                      ;; This is not ideal, but it prevents Pig from blowing up when you use big numbers
                      (and (instance? Long expr)
                           (not (< Integer/MIN_VALUE expr Integer/MAX_VALUE)))
                      (str expr "L")

                      :else (str expr))
     (string? expr) (escape+quote expr)
     (symbol? expr) (if (.startsWith (name expr) "?")
                      (subs (name expr) 1)
                      (if-let [v (scope expr)]
                        (expr->script scope v)
                        (throw (ex-info (str "Unable to resolve symbol " expr) {:expr expr :scope scope}))))
     ;; TODO Verify arities
     ;; TODO Add NOT
     (seq? expr) (case (first expr)

                   clojure.core/let
                   (let [[_ scope body] expr
                         scope (->> scope (partition 2) (map vec) (into {}))]
                     (expr->script scope body))

                   quote
                   (expr->script scope (second expr))

                   (let [[op & exprs] expr
                           exprs' (map (partial expr->script scope) exprs)
                           pig-expr (clojure.string/join (clj->op (name op)) exprs')]
                     (str "(" pig-expr ")")))


     :else (throw (IllegalArgumentException. (str "Unknown expression:" (type expr) " " expr))))))

;; TODO add descriptive comment before each command
(defmulti command->script
  "Converts an individual command into the equivalent Pig script"
  (fn [{:keys [type]} state] type))

;; ********** Util **********

(s/defmethod command->script :field
  [{:keys [field]} :- m/FieldExpr
   {:keys [last-command]}]
  (let [context (some->> last-command name (re-find #"[a-z]+") keyword)]
    [nil
     (format-field context field)]))

(s/defmethod command->script :code
  [{:keys [udf init func args]} :- m/CodeExpr
   {:keys [last-command]}]
  (let [id (raw/pigsym "udf")
        context (some->> last-command name (re-find #"[a-z]+") keyword)
        pig-args (->> args (map (partial format-field context)) (join ", "))
        udf (pigpen.pig.runtime/udf-lookup udf)]
    [(str "DEFINE " id " " udf "(" (escape+quote init) "," (escape+quote func) ");\n\n")
     (str id "(" pig-args ")")]))

(defmethod command->script :register
  [{:keys [jar]}
   state]
  {:pre [(string? jar) (not-empty jar)]}
  (str "REGISTER " jar ";\n\n"))

(defmethod command->script :option
  [{:keys [option value]}
   state]
  {:pre [option value]}
  (str "SET " (name option) " " value ";\n\n"))

;; ********** IO **********

(defmulti storage->script (juxt :type :storage))

(defmethod storage->script [:load :binary]
  [{:keys [fields]}]
  (let [pig-fields (->> fields
                     (map format-field)
                     (join ", "))]
    (str "BinStorage()")))

(defmethod storage->script [:load :string]
  [{:keys [fields]}]
  (let [pig-fields (->> fields
                     (map format-field)
                     (map #(str % ":chararray"))
                     (join ", "))]
    (str "PigStorage('\\n')\n    AS (" pig-fields ")")))

(defmethod storage->script [:store :binary]
  [_]
  (str "BinStorage()"))

(defmethod storage->script [:store :string]
  [_]
  (str "PigStorage()"))

(defn storage->script' [command]
  (str "\n    USING " (storage->script command)))

(s/defmethod command->script :load
  [{:keys [id location] :as command} :- m/Load
   state]
  (let [pig-id (escape-id id)
        pig-storage (storage->script' command)]
    (str pig-id " = LOAD '" location "'" pig-storage ";\n\n")))

(s/defmethod command->script :store
  [{:keys [ancestors location] :as command} :- m/Store
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-storage (storage->script' command)]
    (str "STORE " relation-id " INTO '" location "'" pig-storage ";\n\n")))

;; ********** Map **********

(s/defmethod command->script :projection
  [{:keys [expr flatten alias types] :as p} :- m/Projection
   state]
  (let [[pig-define pig-code] (command->script expr state)
        pig-code (if flatten
                   (str "FLATTEN(" pig-code ")")
                   pig-code)
        pig-schema (as-> alias %
                     (map (fn [a t] (str (name a)
                                         (when t (str ":" (type->pig-type t)))))
                          %
                          (concat types (repeat nil)))
                     (join ", " %)
                     (str " AS (" % ")"))]
    [pig-define (str pig-code pig-schema)]))

(s/defmethod command->script :project
  [{:keys [id ancestors projections opts]} :- m/Project
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-projections (->> projections
                          (mapv #(command->script % (assoc state :last-command (first ancestors)))))
        pig-defines (->> pig-projections (map first) (join))
        pig-projections (->> pig-projections (map second) (join ",\n    "))]
    (str pig-defines pig-id " = FOREACH " relation-id " GENERATE\n    " pig-projections ";\n\n")))

(defmethod command->script :sort-opts
  [{:keys [parallel]} state]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(s/defmethod command->script :sort
  [{:keys [id ancestors key comp opts]} :- m/Sort
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-key (format-field key)
        pig-comp (clojure.string/upper-case (name comp))
        pig-opts (command->script opts state)]
    (str pig-id " = ORDER " relation-id " BY " pig-key " " pig-comp pig-opts ";\n\n")))

(defmethod command->script :rank-opts
  [{:keys [dense]} state]
  (if dense " DENSE"))

(s/defmethod command->script :rank
  [{:keys [id ancestors key comp opts]} :- (assoc m/Rank
                                                 (s/optional-key :key) m/Field
                                                 (s/optional-key :comp) (s/enum :asc :desc))
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-sort (when key
                   (str " BY " (format-field key)
                        " " (clojure.string/upper-case (name comp))))
        pig-opts (command->script opts state)]
    (str pig-id " = RANK " relation-id pig-sort pig-opts ";\n\n")))

;; ********** Filter **********

(s/defmethod command->script :filter
  [{:keys [id ancestors expr]} :- m/Filter
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-expr (expr->script expr)]
    (if pig-expr
      (str pig-id " = FILTER " relation-id " BY " pig-expr ";\n\n")
      (str pig-id " = " relation-id ";\n\n"))))

(s/defmethod command->script :take
  [{:keys [id ancestors n opts]} :- m/Take
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)]
    (str pig-id " = LIMIT " relation-id " " n ";\n\n")))

(s/defmethod command->script :sample
  [{:keys [id ancestors p opts]} :- m/Sample
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)]
    (str pig-id " = SAMPLE " relation-id " " p ";\n\n")))

;; ********** Join **********

(s/defmethod command->script :reduce
  [{:keys [id keys join-types ancestors opts]} :- m/Reduce
   state]
  (let [pig-id (escape-id id)
        relation-id (escape-id (first ancestors))]
    (str pig-id " = COGROUP " relation-id " ALL;\n\n")))

(defmethod command->script :group-opts
  [{:keys [strategy parallel]} state]
  (let [pig-using (if strategy (str " USING '" (name strategy) "'"))
        pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-using pig-parallel)))

(s/defmethod command->script :group
  [{:keys [id keys join-types ancestors opts]} :- m/Group
   state]
  (let [pig-id (escape-id id)
        pig-clauses (join ", "
                          (zip [r ancestors
                                k keys
                                j join-types]
                            (str (escape-id r)
                                 " BY " (format-field k)
                                 (if (= j :required) " INNER"))))
        pig-opts (command->script opts state)]
    (str pig-id " = COGROUP " pig-clauses pig-opts ";\n\n")))

(defmethod command->script :join-opts
  [{:keys [strategy parallel]} state]
  (let [pig-using (if strategy (str " USING '" (name strategy) "'"))
        pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-using pig-parallel)))

(s/defmethod command->script :join
  [{:keys [id keys join-types ancestors opts]} :- m/Join
   state]
  (let [pig-id (escape-id id)
        clauses (zip [r ancestors
                      k keys]
                  (str (escape-id r)
                       " BY " (format-field k)))
        pig-join (case join-types
                   [:required :optional] " LEFT OUTER"
                   [:optional :required] " RIGHT OUTER"
                   [:optional :optional] " FULL OUTER"
                   "")
        pig-clauses (join (str pig-join ", ") clauses)
        pig-opts (command->script opts state)]
    (str pig-id " = JOIN " pig-clauses pig-opts ";\n\n")))

;; ********** Set **********

(defmethod command->script :distinct-opts
  [{:keys [partition-by partition-type parallel]} {:keys [partitioner]}]
  (let [n (when partition-by (swap! partitioner inc))
        pig-set (when partition-by
                  (str "SET PigPenPartitioner" n "_type " (escape+quote (name (or partition-type :frozen))) ";\n"
                       "SET PigPenPartitioner" n "_init '';\n"
                       "SET PigPenPartitioner" n "_func " (escape+quote partition-by) ";\n\n"))
        pig-partition (when partition-by
                        (str " PARTITION BY pigpen.PigPenPartitioner.PigPenPartitioner" n))
        pig-parallel (when parallel (str " PARALLEL " parallel))]
    [pig-set (str pig-partition pig-parallel)]))

(s/defmethod command->script :distinct
  [{:keys [id ancestors opts]} :- m/Distinct
   state]
  (let [relation-id (first ancestors)
        [pig-set pig-opts] (command->script opts state)]
    (str pig-set id " = DISTINCT " relation-id pig-opts ";\n\n")))

(defmethod command->script :concat-opts
  [{:keys [parallel]} state]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(s/defmethod command->script :concat
  [{:keys [id ancestors opts]} :- m/Concat
   state]
  (let [pig-id (escape-id id)
        pig-ancestors (->> ancestors (map escape-id) (join ", "))
        pig-opts (command->script opts state)]
    (str pig-id " = UNION " pig-ancestors pig-opts ";\n\n")))

;; ********** Script **********

(s/defmethod command->script :noop
  [{:keys [id ancestors fields]} :- m/NoOp
   state]
  (let [relation-id (first ancestors)]
    (str id " = FOREACH " relation-id " GENERATE\n    "
         (join ", " (map format-field fields))
         ";\n\n")))

(defmethod command->script :store-many
  [command state]
  "-- Generated by PigPen: https://github.com/Netflix/PigPen\n\n")

(defn commands->script
  "Transforms a sequence of commands into a Pig script"
  [commands]
  (let [state {:partitioner (atom -1)}]
    (apply str
      (for [command commands]
        (command->script command state)))))
