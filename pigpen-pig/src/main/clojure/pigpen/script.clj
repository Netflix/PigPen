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

(ns pigpen.script
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
     (symbol? expr) (if-let [v (scope expr)]
                      (expr->script scope v)
                      (name expr))
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

;; Hadoop doesn't allow for configuration of partitioners, so we make a lot of them
(def num-partitioners 32)

(dotimes [n num-partitioners]
  (eval
    `(gen-class
       :name ~(symbol (str "pigpen.PigPenPartitioner" n))
       :extends pigpen.PigPenPartitioner)))

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
  {:pre [(string? option) value]}
  (str "SET " option " " value ";\n\n"))

;; ********** IO **********

(defmulti storage->script (juxt :type :storage))

(defmethod storage->script [:load :binary]
  [{:keys [fields]}]
  (let [pig-fields (->> fields
                     (map format-field)
                     (join ", "))]
    (str "PigStorage()\n    AS (" pig-fields ")")))

(defmethod storage->script [:load :string]
  [{:keys [fields]}]
  (let [pig-fields (->> fields
                     (map format-field)
                     (map #(str % ":chararray"))
                     (join ", "))]
    (str "PigStorage('\\n')\n    AS (" pig-fields ")")))

(defmethod storage->script [:store :binary]
  [_]
  (str "PigStorage()"))

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
  [{:keys [expr flatten alias implicit-schema]} :- (assoc m/Projection
                                                          (s/optional-key :implicit-schema) (s/maybe s/Bool)
                                                          (s/optional-key :context) s/Keyword)
   state]
  (let [[pig-define pig-code] (command->script expr state)
        pig-code (if flatten
                   (str "FLATTEN(" pig-code ")")
                   pig-code)
        pig-alias (str "(" (join ", " (map name alias)) ")")
        pig-schema (when-not implicit-schema
                     (str " AS " pig-alias))]
    [pig-define (str pig-code pig-schema)]))

(s/defmethod command->script :generate
  [{:keys [id ancestors projections opts]} :- m/Mapcat
   state]
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-projections (->> projections
                          (map #(assoc % :implicit-schema (:implicit-schema opts)))
                          (mapv #(command->script % (assoc state :last-command (first ancestors)))))
        pig-defines (->> pig-projections (map first) (join))
        pig-projections (->> pig-projections (map second) (join ",\n    "))]
    (str pig-defines pig-id " = FOREACH " relation-id " GENERATE\n    " pig-projections ";\n\n")))

(defmethod command->script :order-opts
  [{:keys [parallel]} state]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(s/defmethod command->script :order
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

(s/defmethod command->script :limit
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
                        (str " PARTITION BY pigpen.PigPenPartitioner" n))
        pig-parallel (when parallel (str " PARALLEL " parallel))]
    [pig-set (str pig-partition pig-parallel)]))

(s/defmethod command->script :distinct
  [{:keys [id ancestors opts]} :- m/Distinct
   state]
  (let [relation-id (first ancestors)
        [pig-set pig-opts] (command->script opts state)]
    (str pig-set id " = DISTINCT " relation-id pig-opts ";\n\n")))

(defmethod command->script :union-opts
  [{:keys [parallel]} state]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(s/defmethod command->script :union
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

(defmethod command->script :script
  [command state]
  "-- Generated by PigPen: https://github.com/Netflix/PigPen\n\n")

(defn commands->script
  "Transforms a sequence of commands into a Pig script"
  [commands]
  (let [state {:partitioner (atom -1)}]
    (apply str
      (for [command commands]
        (command->script command state)))))
