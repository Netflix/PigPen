(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine))
  (:require [pigpen.raw :as raw]))

(defn- get-tap-fn [name]
  ({"text" (fn [location] (Hfs. (TextLine.) location))}
   name))

(defn load-text [location]
  (raw/load$ location ["offset" "line"] (raw/storage$ [] "asdf" #_(Hfs. (TextLine.) location) {}) {}))

(defmulti command->flow
          "Converts an individual command into the equivalent Cascading flow definition."
          (fn [{:keys [type]} flow-def] type))


(defmethod command->flow :load
           [{:keys [id location storage fields opts]} flow-def]
  {:pre [id location storage fields]}
  ;(println id location storage fields)
  (assoc flow-def :sources {id ((get-tap-fn (:func storage)) location)})
  )


