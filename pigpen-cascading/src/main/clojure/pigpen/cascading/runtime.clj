(ns pigpen.cascading.runtime
  (:import (org.apache.hadoop.io BytesWritable)
           (pigpen.cascading OperationUtil)
           (java.util ArrayList)
           (clojure.lang ISeq))
  (:require [pigpen.runtime :as rt]
            [pigpen.raw :as raw]
            [pigpen.oven :as oven]
            [taoensso.nippy :refer [freeze thaw]]))

(defmulti hybrid->clojure
          "Converts a hybrid cascading/clojure data structure into 100% clojure.

           DataByteArrays are assumed to be frozen clojure structures.

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
  (mapv hybrid->clojure value))

;; ******* Serialization ********
(defn ^:private cs-freeze [value]
  (BytesWritable. (freeze value {:skip-header? true, :legacy-mode true})))

(defmethod pigpen.runtime/pre-process [:cascading :frozen-with-nils]
           [_ _]
  (fn [args]
    (mapv #(thaw (.getBytes %)) args)))

(defmethod pigpen.runtime/pre-process [:cascading :frozen]
           [_ _]
  (fn [args]
    (mapv hybrid->clojure args)))

(defmethod pigpen.runtime/post-process [:cascading :frozen-with-nils]
           [_ _]
  (fn [args]
    (mapv cs-freeze args)))

(defmethod pigpen.runtime/post-process [:cascading :native-key-frozen-val]
           [_ _]
  (fn [[key value]]
    [key (cs-freeze value)]))
