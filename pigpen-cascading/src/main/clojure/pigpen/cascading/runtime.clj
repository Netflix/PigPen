(ns pigpen.cascading.runtime
  (:import (org.apache.hadoop.io BytesWritable))
  (:require [pigpen.runtime :as rt]
            [pigpen.raw :as raw]
            [pigpen.oven :as oven]
            [taoensso.nippy :refer [freeze thaw]]))


;; ******* Serialization ********

(defmethod pigpen.runtime/pre-process [:cascading :frozen-with-nils]
           [_ _]
  (fn [args]
    (println "preprocess args" args)
    (mapv #(thaw (.getBytes %)) args)))

(defmethod pigpen.runtime/pre-process [:cascading :frozen]
           [_ _]
  (fn [args]
    (println "preprocess args" args)
    (mapv #(thaw (.getBytes %)) args)))

(defmethod pigpen.runtime/post-process [:cascading :frozen-with-nils]
           [_ _]
  (fn [args]
    (println "postprocess args" args)
    (mapv #(BytesWritable. (freeze % {:skip-header? true, :legacy-mode true})) args)))
