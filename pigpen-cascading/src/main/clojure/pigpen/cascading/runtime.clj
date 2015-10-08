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
            [pigpen.runtime :as rt :refer [HybridToClojure]]
            [schema.core :as s]
            [pigpen.model :as m]))

(set! *warn-on-reflection* true)

;; ******** Serialization ********

(extend-protocol HybridToClojure
  BytesWritable
  (rt/hybrid->clojure [value]
    (-> value
      (OperationUtil/getBytes)
      (thaw {:compressor nil
             :encryptor  nil
             :v1-compatibility? false}))))

(defn cs-freeze [value]
  (BytesWritable. (freeze value {:compressor nil, :skip-header? true})))

(defn ^:private cs-freeze-with-nils [value]
  (if-not (nil? value)
    (cs-freeze value)))

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

(defn add-tuple
  "Adds a tuple to the collector; returns the collector."
  [^TupleEntryCollector collector ^Tuple tuple]
  (doto collector
    (.add tuple)))

;; ******** Prepare ********

(defn prepare-expr [expr]
  (case (:type expr)
    :field expr
    :code (-> expr
            (update-in [:init] pigpen.runtime/eval-string)
            (update-in [:func] pigpen.runtime/eval-string))))

(defn prepare-projection [p]
  (when p
    (update-in p [:expr] prepare-expr)))

(defn prepare-projections [ps]
  (mapv prepare-projection ps))

(defn prepare
  "Called from UDFs to deserialize clojure data structures"
  [context]
  (-> context
    pigpen.runtime/eval-string
    (update-in [:projections] prepare-projections)
    (update-in [:func] prepare-projection)))

(def prepare (memoize prepare))

;; ******** Func ********

(defn field-lookup [values arg]
  (cond
    (string? arg) arg
    (symbol? arg) (values arg)
    :else (throw (ex-info "Unknown arg" {:arg arg, :values values}))))

(s/defn eval-field
  [values {:keys [alias expr :- m/FieldExpr]} :- m/Projection]
  (let [{:keys [field]} expr]
    [alias (values field)]))

(s/defn eval-func
  [values
   {:keys [expr alias]} :- m/Projection
   init
   reducef]
  (let [{:keys [func args]} expr
        arg-values (mapv (partial field-lookup values) args)]
    ((func reducef) init arg-values)))

(defn function-operate
  "Called from pigpen.cascading.PigPenFunction"
  [^FunctionCall function-call]
  (let [{:keys [field-projections func fields]} (.getContext function-call)
        values (fn [f]
                 (-> function-call
                   (.getArguments)
                   (.getObject (pr-str f))
                   rt/hybrid->clojure))
        field-values (->> field-projections
                       (map (partial eval-field values))
                       (into {}))]
    (eval-func values func
               (.getOutputCollector function-call)
               (fn [collector fn-result]
                 (let [result (merge field-values (zipmap (:alias func) fn-result))
                       tuple (->tuple (mapv result fields))]
                   (add-tuple collector tuple))))))

;; ******** CoGroup ********

(defn induce-sentinel-nil
  "Induces a sentinel per-relation nil value to match pigpen's join behavior.
Called from pigpen.cascading.InduceSentinelNils"
  [^FunctionCall function-call
   index] ; the index of the relation in the cogroup
  (let [entry (.getArguments function-call)
        output-collector (.getOutputCollector function-call)]
    (if (or (nil? index)
            (not (nil? (.getObject entry 0))))
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
  (let [{:keys [args required rename-fields folds func fields]} (.getContext buffer-call)

        ;; where to find the arg values in the data
        field-indexes (-> buffer-call
                        (.getJoinerClosure)
                        (.getValueFields)
                        (field-indexes))

        ;; fetch the values for the args
        values (memoize
                 (fn [arg]
                   (let [arg' (get rename-fields arg arg)]
                     (arg->value buffer-call folds field-indexes arg'))))]

    ;; when we have all required values, apply the user function
    (when (every? values required)
      (eval-func values func
                 (.getOutputCollector buffer-call)
                 (fn [collector fn-result]
                   (let [result (zipmap (:alias func) fn-result)
                         tuple (->tuple (mapv result fields))]
                     (add-tuple collector tuple)))))))

;; ******** Reduce ********

(defn reduce-operate
  "Called from pigpen.cascading.ReduceBuffer"
  [^BufferCall buffer-call]
  (let [{:keys [func fields]} (.getContext buffer-call)
        values (->> buffer-call
                 (.getArgumentsIterator)
                 (SingleIterationSeq/create)
                 (map (fn [^TupleEntry e]
                        (rt/hybrid->clojure (.getObject e 0))))
                 constantly)]
    (eval-func values func
               (.getOutputCollector buffer-call)
               (fn [collector fn-result]
                 (let [result (zipmap (:alias func) fn-result)
                       tuple (->tuple (mapv result fields))]
                   (add-tuple collector tuple))))))

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
