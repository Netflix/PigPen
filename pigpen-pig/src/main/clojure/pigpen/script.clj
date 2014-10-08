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
See pigpen.core and pigpen.exec
"
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :refer [join replace]]
            [pigpen.raw :as raw]
            [pigpen.pig.runtime]))

(set! *warn-on-reflection* true)

(defn ^:private format-field [field]
  (cond
    (string? field) (str "'" (replace field "'" "\\'") "'")
    (symbol? field) (str field)
    (number? field) (str "$" field)
    (sequential? field)
    (let [[relation dereference] field]
      (str (clojure.string/join "::" relation)
           (if dereference (str "." dereference))))))

(defn ^:private escape+quote [f]
  (str "'" (if f
             (-> f
               (clojure.string/replace "\\" "\\\\")
               (clojure.string/replace "'" "\\'"))) "'"))

(defn ^:private escape-id [id]
  (clojure.string/replace id "-" "_"))

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

(defmethod command->script :code
  [{:keys [udf expr args]} state]
  {:pre [udf expr args]}
  (let [id (raw/pigsym "udf")
        {:keys [init func]} expr
        pig-args (->> args (map format-field) (join ", "))
        udf (pigpen.pig.runtime/udf-lookup udf)]
    [(str "DEFINE " id " " udf "(" (escape+quote init) "," (escape+quote func) ");\n\n")
     (str id "(" pig-args ")")]))

(defmethod command->script :register
  [{:keys [jar]} state]
  {:pre [(string? jar) (not-empty jar)]}
  (str "REGISTER " jar ";\n\n"))

(defmethod command->script :option
  [{:keys [option value]} state]
  {:pre [(string? option) value]}
  (str "SET " option " " value ";\n\n"))

;; ********** IO **********

(defmethod command->script :storage
  [{:keys [func args]} state]
  {:pre [func args]}
  (let [pig-args (->> args (map #(str "'" % "'")) (join ", "))]
    (str "\n    USING " func "(" pig-args ")")))

(defmulti storage->script (juxt :type :storage))

(defmethod storage->script [:load :binary]
  [{:keys [fields]}]
  (let [pig-fields (->> fields
                     (join ", "))]
    (str "PigStorage()\n    AS (" pig-fields ")")))

(defmethod storage->script [:load :string]
  [{:keys [fields]}]
  (let [pig-fields (->> fields
                     (map #(str % ":chararray"))
                     (join ", "))]
    (str "PigStorage(\\n)\n    AS (" pig-fields ")")))

(defmethod storage->script [:store :binary]
  [_]
  (str "PigStorage()"))

(defmethod storage->script [:store :string]
  [_]
  (str "PigStorage()"))

(defn storage->script' [command]
  (str "\n    USING " (storage->script command)))

(defmethod command->script :load
  [{:keys [id location storage fields opts] :as command} state]
  {:pre [id location storage fields]}
  (let [pig-id (escape-id id)
        pig-storage (storage->script' command)]
    (str pig-id " = LOAD '" location "'" pig-storage ";\n\n")))

(defmethod command->script :store
  [{:keys [id ancestors location storage opts] :as command} state]
  {:pre [id ancestors location storage]}
  (let [relation-id (escape-id (first ancestors))
        pig-storage (storage->script' command)]
    (str "STORE " relation-id " INTO '" location "'" pig-storage ";\n\n")))

;; ********** Map **********

(defmethod command->script :projection-field
  [{:keys [field alias]} state]
  {:pre [field alias]}
  (let [pig-field (format-field field)
        pig-schema (str " AS " alias)]
    [nil (str pig-field pig-schema)]))

(defmethod command->script :projection-func
  [{:keys [code alias]} state]
  {:pre [code alias]}
  (let [[pig-define pig-code] (command->script code state)
        pig-schema (str " AS " alias)]
    [pig-define (str pig-code pig-schema)]))

(defmethod command->script :projection-flat
  [{:keys [code alias implicit-schema]} state]
  {:pre [code alias]}
  (let [[pig-define pig-code] (command->script code state)
        pig-code (str "FLATTEN(" pig-code ")")
        pig-schema (if-not implicit-schema (str " AS " alias))]
    [pig-define (str pig-code pig-schema)]))

(defmethod command->script :generate
  [{:keys [id ancestors projections opts]} state]
  {:pre [id ancestors (not-empty projections)]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-projections (->> projections
                              (map #(assoc % :implicit-schema (:implicit-schema opts)))
                              (mapv #(command->script % state)))
        pig-defines (->> pig-projections (map first) (join))
        pig-projections (->> pig-projections (map second) (join ",\n    "))]
    (str pig-defines pig-id " = FOREACH " relation-id " GENERATE\n    " pig-projections ";\n\n")))

(defmethod command->script :order-opts
  [{:keys [parallel]} state]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(defmethod command->script :order
  [{:keys [id ancestors sort-keys opts]} state]
  {:pre [id ancestors (not-empty sort-keys)]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-clauses (->> sort-keys
                      (partition 2)
                      (map (fn [[key order]] (str key " " (clojure.string/upper-case (name order)))))
                      (clojure.string/join ", "))
        pig-opts (command->script opts state)]
    (str pig-id " = ORDER " relation-id " BY " pig-clauses pig-opts ";\n\n")))

(defmethod command->script :rank-opts
  [{:keys [dense]} state]
  (if dense " DENSE"))

(defmethod command->script :rank
  [{:keys [id ancestors sort-keys opts]} state]
  {:pre [id ancestors]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-clauses (if (not-empty sort-keys)
                      (->> sort-keys
                        (partition 2)
                        (map (fn [[key order]] (str key " " (clojure.string/upper-case (name order)))))
                        (clojure.string/join ", ")
                        (str " BY ")))
        pig-opts (command->script opts state)]
    (str pig-id " = RANK " relation-id pig-clauses pig-opts ";\n\n")))

;; ********** Filter **********

(defmethod command->script :filter
  [{:keys [id ancestors code]} state]
  {:pre [id ancestors code]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        [pig-define pig-code] (command->script code state)]
    (str pig-define pig-id " = FILTER " relation-id " BY " pig-code ";\n\n")))

(defmethod command->script :filter-native
  [{:keys [id ancestors expr]} state]
  {:pre [id ancestors]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-expr (expr->script expr)]
    (if pig-expr
      (str pig-id " = FILTER " relation-id " BY " pig-expr ";\n\n")
      (str pig-id " = " relation-id ";\n\n"))))

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

(defmethod command->script :distinct
  [{:keys [id ancestors opts]} state]
  {:pre [id ancestors]}
  (let [relation-id (first ancestors)
        [pig-set pig-opts] (command->script opts state)]
    (str pig-set id " = DISTINCT " relation-id pig-opts ";\n\n")))

(defmethod command->script :limit
  [{:keys [id ancestors n opts]} state]
  {:pre [id ancestors n opts]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)]
    (str pig-id " = LIMIT " relation-id " " n ";\n\n")))

(defmethod command->script :sample
  [{:keys [id ancestors p opts]} state]
  {:pre [id ancestors p opts]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)]
    (str pig-id " = SAMPLE " relation-id " " p ";\n\n")))

;; ********** Set **********

(defmethod command->script :union-opts
  [{:keys [parallel]} state]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

;; TODO fix dupes in union
(defmethod command->script :union
  [{:keys [id ancestors opts]} state]
  {:pre [id ancestors]}
  (let [pig-id (escape-id id)
        pig-ancestors (->> ancestors (map escape-id) (join ", "))
        pig-opts (command->script opts state)]
    (str pig-id " = UNION " pig-ancestors pig-opts ";\n\n")))

;; ********** Join **********

(defmethod command->script :group-opts
  [{:keys [strategy parallel]} state]
  (let [pig-using (if strategy (str " USING '" (name strategy) "'"))
        pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-using pig-parallel)))

(defmethod command->script :group
  [{:keys [id keys join-types ancestors opts]} state]
  {:pre [id keys join-types ancestors]}
  (let [pig-id (escape-id id)
        clauses (if (= keys [:pigpen.raw/group-all])
                  [(str (escape-id (first ancestors)) " ALL")]
                  (map (fn [r k j] (str (escape-id r) " BY (" (->> k (map format-field) (join ", ")) ")"
                                        (if (= j :required) " INNER")))
                       ancestors keys join-types))
        pig-clauses (join ", " clauses)
        pig-opts (command->script opts state)]
    (str pig-id " = COGROUP " pig-clauses pig-opts ";\n\n")))

(defmethod command->script :join-opts
  [{:keys [strategy parallel]} state]
  (let [pig-using (if strategy (str " USING '" (name strategy) "'"))
        pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-using pig-parallel)))

;; TODO fix self-join
(defmethod command->script :join
  [{:keys [id keys join-types ancestors opts]} state]
  {:pre [id keys join-types ancestors]}
  (let [pig-id (escape-id id)
        clauses (map (fn [r k] (str (escape-id r) " BY (" (->> k (map format-field) (join ", ")) ")"))
                     ancestors keys)
        pig-join (case join-types
                   [:required :optional] " LEFT OUTER"
                   [:optional :required] " RIGHT OUTER"
                   [:optional :optional] " FULL OUTER"
                   "")
        pig-clauses (join (str pig-join ", ") clauses)
        pig-opts (command->script opts state)]
    (str pig-id " = JOIN " pig-clauses pig-opts ";\n\n")))

;; ********** Script **********

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
