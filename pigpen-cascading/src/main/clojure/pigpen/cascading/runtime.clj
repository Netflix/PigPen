;;
;;
;;  Copyright 2015 Netflix, Inc.
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

(ns pigpen.cascading.runtime
  (:import (java.util List)
           (org.apache.hadoop.io BytesWritable)
           (cascading.operation OperationCall FunctionCall
                                BufferCall AggregatorCall)
           (cascading.tuple Fields Tuple TupleEntry TupleEntryCollector)
           (pigpen.cascading OperationUtil SingleIterationSeq))
  (:require [taoensso.nippy :refer [freeze thaw]]
            [pigpen.runtime :as rt]
            [schema.core :as s]
            [pigpen.model :as m]))

(set! *warn-on-reflection* true)

;; ******** Serialization ********

(defmethod rt/hybrid->clojure BytesWritable [value]
  (-> value (OperationUtil/getBytes) thaw))

(defn cs-freeze [value]
  (BytesWritable. (freeze value {:skip-header? true, :legacy-mode true})))

(defn ^:private cs-freeze-with-nils [value]
  (if value (cs-freeze value)))

(defmethod pigpen.runtime/post-process [:cascading :native]
  [_ _]
  identity)

(defmethod pigpen.runtime/post-process [:cascading :frozen]
  [_ _]
  (fn [args]
    (map cs-freeze args)))

(defmethod pigpen.runtime/post-process [:cascading :frozen-with-nils]
  [_ _]
  (fn [args]
    (map cs-freeze-with-nils args)))

(defmethod pigpen.runtime/post-process [:cascading :native-key-frozen-val]
  [_ _]
  (fn [[key value]]
    [key (cs-freeze value)]))

(defn ^:private ^Tuple ->tuple [^List l]
  (Tuple. (.toArray l)))

;; ******** Prepare ********

(defn prepare-expr [expr]
  (case (:type expr)
    :field expr
    :code (-> expr
            (update-in [:init] pigpen.runtime/eval-string)
            (update-in [:func] pigpen.runtime/eval-string))))

(defn prepare-projections [ps]
  (mapv #(update-in % [:expr] prepare-expr) ps))

(defn prepare
  "Called from UDFs to deserialize clojure data structures"
  [context]
  (-> context
    pigpen.runtime/eval-string
    (update-in [:projections] prepare-projections)))

(def prepare (memoize prepare))

;; ******** Func ********

(defn cross-product [[head & more]]
  (if head
    (for [value head
          child (cross-product more)]
      (merge child value))
    [{}]))

(defmulti eval-func (fn [udf f args] udf))

(defmethod eval-func :seq
  [_ f args]
  (f args))

(defmulti eval-expr
  (fn [expr values]
    (:type expr)))

(s/defmethod eval-expr :field
  [{:keys [field]} :- m/FieldExpr
   values]
  [(values field)])

(defn field-lookup [values arg]
  (cond
    (string? arg) arg
    (symbol? arg) (values arg)
    :else (throw (ex-info "Unknown arg" {:arg arg, :values values}))))

(s/defmethod eval-expr :code
  [{:keys [udf func args]} :- m/CodeExpr
   values]
  (let [arg-values (mapv (partial field-lookup values) args)]
    (eval-func udf func arg-values)))

(s/defn eval-projections
  [values {:keys [expr flatten alias]} :- m/Projection]
  (let [result (eval-expr expr values)]
    (if flatten
      (map (partial zipmap alias) result)
      (zipmap alias result))))

(defn function-operate
  "Called from pigpen.cascading.PigPenFunction"
  [^FunctionCall function-call]
  (let [{:keys [projections fields]} (.getContext function-call)
        tuple-entry (.getArguments function-call)
        values (fn [f]
                 (rt/hybrid->clojure
                   (.getObject tuple-entry (pr-str f))))]
    (doseq [r (->> projections
                (map (partial eval-projections values))
                (cross-product)
                (map #(mapv % fields)))]
      (-> function-call
        (.getOutputCollector)
        (.add (->tuple r))))))

;; ******** CoGroup ********

(defn induce-sentinel-nil
  "Induces a sentinel per-relation nil value to match pigpen's join behavior.
Called from pigpen.cascading.InduceSentinelNils"
  [^FunctionCall function-call
   index] ; the index of the relation in the cogroup
  (let [entry (.getArguments function-call)
        output-collector (.getOutputCollector function-call)]
    (if (or (nil? index)
            (.getObject entry 0))
      (.add output-collector entry)
      (.add output-collector
        (doto (Tuple.)
          (.add (BytesWritable. (byte-array [index])))
          (.add (.getObject entry 1)))))))

(defn remove-sentinel-nil
  "Revert the sentinel nils introduced by `induce-sentinel-nil`"
  ;; Assumes that a frozen user value will alwyas be more than 1 byte
  [^BytesWritable v]
  (when (and v (< 1 (.getLength v)))
    v))

(defn field-indexes
  "Takes an array of Fields and creates a lookup from arg -> [iterator, tuple index]"
  [value-fields]
  (->> value-fields
    (map-indexed
      (fn [iterator-index ^Fields fields]
        (->> fields
          (map-indexed
            (fn [field-index f]
              [(symbol f) [iterator-index field-index]])))))
    (apply concat)
    (into {})))

(defn arg->value
  "For a given arg, finds the value as an iterator or group key"
  [^BufferCall buffer-call folds field-indexes arg]
  (let [fold-selector (if (contains? folds arg)
                        first
                        seq)]
    (if-let [[iterator-index field-index] (get field-indexes arg)]
      (as-> buffer-call %
        (.getJoinerClosure %)
        (.getIterator % iterator-index)
        (SingleIterationSeq/create %)
        (map #(.getObject ^Tuple % field-index) %)
        (map rt/hybrid->clojure %)
        (fold-selector %))

      ;else group
      (->> buffer-call
        (.getGroup)
        (.getTuple)
        (some identity)
        remove-sentinel-nil
        rt/hybrid->clojure))))

(defn group-operate
  "Called from pigpen.cascading.GroupBuffer"
  [^BufferCall buffer-call]
  (let [{:keys [projections fields ancestor]} (.getContext buffer-call)

        ;; the list of args required by this projection
        args (-> projections
               first
               (get-in [:expr :args]))

        ;; where to find the arg values in the data
        field-indexes (-> buffer-call
                        (.getJoinerClosure)
                        (.getValueFields)
                        (field-indexes))

        ;; a list of which fields are required. This is to compute inner/outer groups
        required (mapcat (fn [a j] (when (= j :required) [a]))
                         (next args)
                         (:join-types ancestor))

        ;; folds add an extra layer of indirection to field names; this resolves it
        rename-fields (some->> ancestor
                        :fold
                        :projections
                        (map (fn [p]
                               [(-> p :alias first)
                                (or (-> p :expr :field)
                                    (-> p :expr :args first))]))
                        (into {}))

        ;; This exists becasue we use this for both fold and non-fold co-groupings.
        ;; For relations that have been folded already, there will only be one value,
        ;; so we take the first. This identifies which relations have been folded.
        folds (some->> ancestor
                :fold
                :projections
                (filter (comp #{:fold} :udf :expr))
                (map (comp first :alias))
                (map rename-fields)
                set)

        ;; fetch the values for these args
        values (->> args
                 (map (fn [arg]
                        (let [arg' (get rename-fields arg arg)]
                          [arg (arg->value buffer-call folds field-indexes arg')])))
                 (into {}))]

    ;; when we have all required values, apply the user function
    (when (every? values required)
      (doseq [r (->>
                  (eval-projections values (first projections))
                  (map #(mapv % fields)))]
        (-> buffer-call
          (.getOutputCollector)
          (.add (->tuple r)))))))

;; ******** Reduce ********

(defn reduce-operate
  "Called from pigpen.cascading.ReduceBuffer"
  [^BufferCall buffer-call]
  (let [{:keys [projections fields ancestor]} (.getContext buffer-call)
        values (->> buffer-call
                 (.getArgumentsIterator)
                 (SingleIterationSeq/create)
                 (map (fn [^TupleEntry e]
                        (rt/hybrid->clojure (.getObject e 0)))))]
    (doseq [r (->>
                (eval-projections {(:arg ancestor) values} (first projections))
                (map #(mapv % fields)))]
      (-> buffer-call
        (.getOutputCollector)
        (.add (->tuple r))))))

;; ******** Fold ********

; Called from pigpen.cascading.PigPenAggregateBy

(defn context->fold-fn [context part]
  (get-in context [:projections 0 :expr :func part]))

(defn aggregate-partial-aggregate
  [context ^TupleEntry args ^Tuple agg]
  (if-not agg
    (let [combinef (context->fold-fn context :combinef)]
      (recur context args (->tuple [(combinef)])))

    (let [pre (context->fold-fn context :pre)
          reducef (context->fold-fn context :reducef)
          agg (.getObject agg 0)]
      (->>
        (.getObject args 0)
        rt/hybrid->clojure
        vector
        pre
        (reduce reducef agg)))))

(defn aggregate-partial-complete
  [^Tuple agg]
  (-> agg
    (.getObject 0)
    cs-freeze
    vector
    ->tuple))

(defn aggregate-final-start
  [context ^AggregatorCall aggregator-call]
  (let [combinef (context->fold-fn context :combinef)]
    (combinef)))

(defn aggregate-final-aggregate
  [context ^AggregatorCall aggregator-call]
  (let [combinef (context->fold-fn context :combinef)
        agg (.getContext aggregator-call)
        arg (-> aggregator-call
              (.getArguments)
              (.getObject 0)
              rt/hybrid->clojure)]
    (combinef agg arg)))

(defn aggregate-final-complete
  [context ^AggregatorCall aggregator-call]
  (let [post (context->fold-fn context :post)
        agg (.getContext aggregator-call)
        value (-> agg
                post
                cs-freeze
                vector
                ->tuple)]
    (-> aggregator-call
      (.getOutputCollector)
      (.add value))))
