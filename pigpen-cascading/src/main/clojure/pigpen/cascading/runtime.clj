(ns pigpen.cascading.runtime
  (:import (org.apache.hadoop.io BytesWritable)
           (pigpen.cascading OperationUtil SingleIterationSeq)
           (cascading.tuple TupleEntryCollector Tuple TupleEntry)
           (java.util List))
  (:require [pigpen.runtime :as rt]
            [pigpen.raw :as raw]
            [pigpen.oven :as oven]
            [taoensso.nippy :refer [freeze thaw]]))

(defn hybrid->clojure [value]
  (if (instance? BytesWritable value)
    (-> value (OperationUtil/getBytes) thaw)
    value))

;; ******* Serialization ********
(defn ^:private cs-freeze [value]
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

(defn- wrap-iterator [it]
  (SingleIterationSeq/create it))

(defn emit-tuples
  "Given a seq containing the results of an operation, emit the corresponding cascading tuples."
  [seq ^TupleEntryCollector collector]
  (doseq [r seq] (.add collector (Tuple. (.toArray r)))))

(defn emit-group-buffer-tuples
  "Emit the results from a GroupBuffer."
  [funcs key iterators ^TupleEntryCollector collector group-all udf-type key-separate-from-value]
  ; TODO: handle :combinef
  (let [normal-fn #(let [f (first funcs)]
                    (if (or group-all (not key-separate-from-value))
                      (f (map wrap-iterator iterators))
                      (f (concat [key] (map wrap-iterator iterators)))))
        algebraic-fn #(let [vals (map (fn [{:keys [pre combinef reducef post]} it]
                                        (->> (wrap-iterator it)
                                             pre
                                             (reduce reducef (combinef))
                                             post))
                                      funcs iterators)]
                       (if group-all
                         [vals]
                         [(cons key vals)]))
        result (if (= :algebraic udf-type)
                 (algebraic-fn)
                 (normal-fn))]
    (emit-tuples result collector)))

(defn emit-join-buffer-tuples
  "Emit the results from a JoinBuffer."
  [f iterator ^TupleEntryCollector collector all-args]
  (doseq [^List t (wrap-iterator iterator)]
    ; The incoming tuple contains <key1, value1, key2, value2>. Unless all-args is true, the function only
    ; cares about the values, hence the indices are 1 and 3
    (let [result (f (if all-args t
                                 [(.get t 1) (.get t 3)]))]
      (emit-tuples result collector))))

(defn emit-function-tuples
  "Emit the results from a PigPenFunction."
  [f ^List tuple ^TupleEntryCollector collector]
  (emit-tuples (f tuple) collector))

(defn get-seed-value
  "Generate the seed value for a partial aggregation."
  [{:keys [combinef]}]
  (combinef))

(defn compute-partial-mapper
  "Compute the result of a partial aggregation (map-side)."
  [{:keys [pre reducef post]} arg acc]
  (println "mapper" arg acc)
  (println "post" post)
  ; TODO: pre should not require wrapping the arg like this.
  (Tuple. (to-array [(reducef acc (first (pre [arg])))])))

(defn compute-partial-reducer
  "Compute the result of a partial aggregation (map-side)."
  [{:keys [pre combinef post]} arg acc]
  (println "reducer" arg acc)
  (combinef acc arg))
