(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine))
  (:require [pigpen.raw :as raw]))

(defn- get-tap-fn [name]
  ({"text" (fn [location] (Hfs. (TextLine.) location))}
   name))

(defn load-text [location]
  (raw/load$ location ["offset" "line"] (raw/storage$ [] "text" {}) {}))

(defmulti command->flowdef
          "Converts an individual command into the equivalent Cascading flow definition."
          (fn [{:keys [type]} flowdef] type))


(defmethod command->flowdef :load
           [{:keys [id location storage fields opts]} flowdef]
  {:pre [id location storage fields]}
  (update-in flowdef [:sources] (partial merge {id ((get-tap-fn (:func storage)) location)}))
  )

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [commands]
  (let [flowdef (reduce (fn [def cmd] (command->flowdef cmd def)) {} commands)]
    flowdef))

