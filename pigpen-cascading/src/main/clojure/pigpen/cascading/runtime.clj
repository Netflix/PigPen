(ns pigpen.cascading.runtime
  (:import (org.apache.hadoop.io BytesWritable)
           (pigpen.cascading OperationUtil)
           (java.util ArrayList)
           (clojure.lang ISeq)
           (cascading.tuple TupleEntryCollector Tuple TupleEntry))
  (:require [pigpen.runtime :as rt]
            [pigpen.raw :as raw]
            [pigpen.oven :as oven]
            [taoensso.nippy :refer [freeze thaw]]))

(defn emit-tuples
  "Given a seq containing the results of an operation, emit the corresponding cascading tuples."
  [seq ^TupleEntryCollector collector]
  (doseq [r seq] (.add collector (Tuple. (.toArray r)))))

(defn emit-group-buffer-tuples
  "Emit the results from a GroupBuffer."
  [f key iterators ^TupleEntryCollector collector]
  (let [result (f (concat [key] (map iterator-seq iterators)))]
    (emit-tuples result collector)))

(defn emit-join-buffer-tuples
  "Emit the results from a JoinBuffer."
  [f iterator ^TupleEntryCollector collector]
  (doseq [^TupleEntry t (iterator-seq iterator)]
    ; The incoming tuple contains <key1, value1, key2, value2>. The function only cares about the values, hence
    ; the indices are 1 and 3
    (let [result (f [(.getObject t 1) (.getObject t 3)])]
      (emit-tuples result collector))))

(defn emit-function-tuples
  "Emit the results from a PigPenFunction."
  [f ^Tuple tuple ^TupleEntryCollector collector]
  (emit-tuples (f tuple) collector))

(defmulti hybrid->clojure
          "Converts a hybrid cascading/clojure data structure into 100% clojure.

           ByteWritables are assumed to be frozen clojure structures.

           Only raw pig types are expected here - anything normal (Boolean, String, etc)
           should be frozen."
          type)

(defmethod hybrid->clojure :default [value]
  (throw (IllegalStateException. (str "Unexpected value:" value))))

(defmethod hybrid->clojure nil [value]
  ;; nils are allowed because they occur in joins
  nil)

(defmethod hybrid->clojure Number [value]
  ;; Numbers are allowed because RANK produces them
  value)

(defmethod hybrid->clojure BytesWritable [^BytesWritable value]
  (-> value (OperationUtil/getBytes) thaw))

(defmethod hybrid->clojure ISeq [^ISeq value]
  (map hybrid->clojure value))

;; ******* Serialization ********
(defn ^:private cs-freeze [value]
  (BytesWritable. (freeze value {:skip-header? true, :legacy-mode true})))

(defn ^:private cs-freeze-with-nils [value]
  (if value (cs-freeze value)))

(defmethod pigpen.runtime/pre-process [:cascading :frozen]
  [_ _]
  (fn [args]
    (map hybrid->clojure args)))

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
