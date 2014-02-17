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
  (:require [clojure.string :refer [join replace]]))

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
     (number? expr) (str expr)
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

;; TODO add descriptive comment before each command
(defmulti command->script
  "Converts an individual command into the equivalent Pig script"
  :type)

;; ********** Util **********

(defmethod command->script :expr
  [{:keys [init func]}]
  (->> [init func] (map escape+quote) (join ", ")))

(defmethod command->script :code
  [{:keys [return expr args]}]
  {:pre [return expr args]}
  (let [pig-code (command->script expr)
        pig-args (->> args (map format-field) (cons pig-code) (join ", "))]
    (str "pigpen.PigPenFn" return "(" pig-args ")")))

(defmethod command->script :register
  [{:keys [jar]}]
  {:pre [(string? jar) (not-empty jar)]}
  (str "REGISTER " jar ";\n\n"))

(defmethod command->script :option
  [{:keys [option value]}]
  {:pre [(string? option) value]}
  (str "SET " option " " value ";\n\n"))

;; ********** IO **********

(defmethod command->script :storage
  [{:keys [func args]}]
  {:pre [func args]}
  (let [pig-args (->> args (map #(str "'" % "'")) (join ", "))]
    (str "\n    USING " func "(" pig-args ")")))

(defmethod command->script :load
  [{:keys [id location storage fields opts]}]
  {:pre [id location storage fields]}
  (let [pig-id (escape-id id)
        pig-fields (->> fields
                     (map (if-let [cast (:cast opts)] #(str % ":" cast) identity))
                     (join ", "))
        pig-storage (command->script storage)
        pig-schema (if-not (:implicit-schema opts) (str "\n    AS (" pig-fields ")"))]
    (str pig-id " = LOAD '" location "'" pig-storage pig-schema ";\n\n")))

(defmethod command->script :store
  [{:keys [id ancestors location storage opts]}]
  {:pre [id ancestors location storage]}
  (let [relation-id (escape-id (first ancestors))
        pig-storage (command->script storage)]
    (str "STORE " relation-id " INTO '" location "'" pig-storage ";\n\n")))

;; ********** Map **********

(defmethod command->script :projection-field
  [{:keys [field alias]}]
  {:pre [field alias]}
  (let [pig-field (format-field field)
        pig-schema (str " AS " alias)]
    (str pig-field pig-schema)))

(defmethod command->script :projection-func
  [{:keys [code alias]}]
  {:pre [code alias]}
  (let [pig-code (command->script code)
        pig-schema (str " AS " alias)]
    (str pig-code pig-schema)))

(defmethod command->script :projection-flat
  [{:keys [code alias implicit-schema]}]
  {:pre [code alias]}
  (let [pig-code (str "FLATTEN(" (command->script code) ")")
        pig-schema (if-not implicit-schema (str " AS " alias))]
    (str pig-code pig-schema)))

(defmethod command->script :generate
  [{:keys [id ancestors projections opts]}]
  {:pre [id ancestors (not-empty projections)]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-projections (as-> projections %
                              (map #(assoc % :implicit-schema (:implicit-schema opts)) %)
                              (map command->script %)
                              (join ",\n    " %))]
    (str pig-id " = FOREACH " relation-id " GENERATE\n    " pig-projections ";\n\n")))

(defmethod command->script :order-opts
  [{:keys [parallel]}]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(defmethod command->script :order
  [{:keys [id ancestors sort-keys opts]}]
  {:pre [id ancestors (not-empty sort-keys)]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-clauses (->> sort-keys
                      (partition 2)
                      (map (fn [[key order]] (str key " " (clojure.string/upper-case (name order)))))
                      (clojure.string/join ", "))
        pig-opts (command->script opts)]
    (str pig-id " = ORDER " relation-id " BY " pig-clauses pig-opts ";\n\n")))

(defmethod command->script :rank-opts
  [{:keys [dense]}]
  (if dense " DENSE"))

(defmethod command->script :rank
  [{:keys [id ancestors sort-keys opts]}]
  {:pre [id ancestors]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-clauses (if (not-empty sort-keys)
                      (->> sort-keys
                        (partition 2)
                        (map (fn [[key order]] (str key " " (clojure.string/upper-case (name order)))))
                        (clojure.string/join ", ")
                        (str " BY ")))
        pig-opts (command->script opts)]
    (str pig-id " = RANK " relation-id pig-clauses pig-opts ";\n\n")))

;; ********** Filter **********

(defmethod command->script :filter
  [{:keys [id ancestors code]}]
  {:pre [id ancestors code]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-code (command->script code)]
    (str pig-id " = FILTER " relation-id " BY " pig-code ";\n\n")))

(defmethod command->script :filter-native
  [{:keys [id ancestors expr]}]
  {:pre [id ancestors]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)
        pig-expr (expr->script expr)]
    (if pig-expr
      (str pig-id " = FILTER " relation-id " BY " pig-expr ";\n\n")
      (str pig-id " = " relation-id ";\n\n"))))

(defmethod command->script :distinct-opts
  [{:keys [parallel]}]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(defmethod command->script :distinct
  [{:keys [id ancestors opts]}]
  {:pre [id ancestors]}
  (let [relation-id (first ancestors)
        pig-opts (command->script opts)]
    (str id " = DISTINCT " relation-id pig-opts ";\n\n")))

(defmethod command->script :limit
  [{:keys [id ancestors n opts]}]
  {:pre [id ancestors n opts]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)]
    (str pig-id " = LIMIT " relation-id " " n ";\n\n")))

(defmethod command->script :sample
  [{:keys [id ancestors p opts]}]
  {:pre [id ancestors p opts]}
  (let [relation-id (escape-id (first ancestors))
        pig-id (escape-id id)]
    (str pig-id " = SAMPLE " relation-id " " p ";\n\n")))

;; ********** Set **********

(defmethod command->script :union-opts
  [{:keys [parallel]}]
  (let [pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-parallel)))

(defmethod command->script :union
  [{:keys [id ancestors opts]}]
  {:pre [id ancestors]}
  (let [pig-id (escape-id id)
        pig-ancestors (->> ancestors (map escape-id) (join ", "))
        pig-opts (command->script opts)]
    (str pig-id " = UNION " pig-ancestors pig-opts ";\n\n")))

;; ********** Join **********

(defmethod command->script :group-opts
  [{:keys [strategy parallel]}]
  (let [pig-using (if strategy (str " USING '" (name strategy) "'"))
        pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-using pig-parallel)))

(defmethod command->script :group
  [{:keys [id keys join-types ancestors opts]}]
  {:pre [id keys join-types ancestors]}
  (let [pig-id (escape-id id)
        clauses (if (= keys [:pigpen.raw/group-all])
                  [(str (escape-id (first ancestors)) " ALL")]
                  (map (fn [r k j] (str (escape-id r) " BY (" (->> k (map format-field) (join ", ")) ")"
                                        (if (= j :required) " INNER")))
                       ancestors keys join-types))
        pig-clauses (join ", " clauses)
        pig-opts (command->script opts)]
    (str pig-id " = COGROUP " pig-clauses pig-opts ";\n\n")))

(defmethod command->script :join-opts
  [{:keys [strategy parallel]}]
  (let [pig-using (if strategy (str " USING '" (name strategy) "'"))
        pig-parallel (if parallel (str " PARALLEL " parallel))]
    (str pig-using pig-parallel)))

(defmethod command->script :join
  [{:keys [id keys join-types ancestors opts]}]
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
        pig-opts (command->script opts)]
    (str pig-id " = JOIN " pig-clauses pig-opts ";\n\n")))

;; ********** Script **********

(defmethod command->script :script [command]
  "-- Generated by pig-pen\n\n")
