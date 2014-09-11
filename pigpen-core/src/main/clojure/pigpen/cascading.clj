(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine)
           (cascading.pipe Pipe))
  (:require [pigpen.raw :as raw]))

(defn- get-tap-fn [name]
  (let [tap ({"text" (fn [location] (Hfs. (TextLine.) location))}
             name)]
    (if (nil? tap)
      (throw (Exception. (str "Unrecognized tap type: " name)))
      tap)))

(defn load-text [location]
  (raw/load$ location ["offset" "line"] (raw/storage$ [] "text" {}) {}))

(defmulti command->flowdef
          "Converts an individual command into the equivalent Cascading flow definition."
          (fn [{:keys [type]} flowdef] type))

(defmethod command->flowdef :register
           [command flowdef]
  flowdef)

(defmethod command->flowdef :load
           [{:keys [id location storage fields opts]} flowdef]
  {:pre [id location storage fields]}
  (update-in flowdef [:sources] (partial merge {id ((get-tap-fn (:func storage)) location)})))

(defmethod command->flowdef :store
           [{:keys [id ancestors location storage opts]} flowdef]
  {:pre [id ancestors location storage]}
  (let [tap ((get-tap-fn (:func storage)) location)]
    (-> flowdef
        (update-in [:pipe-to-sink] (partial merge {(first ancestors) tap}))
        (update-in [:sinks] (partial merge {id tap})))))

(defmethod command->flowdef :generate
           [{:keys [id ancestors projections opts]} flowdef]
  {:pre [id ancestors (not-empty projections)]}
  (if (contains? (:sources flowdef) (first ancestors))
    (let [pipe (Pipe. (str id))]
      (-> flowdef
          (update-in [:source-to-pipe] (partial merge {(first ancestors) pipe}))
          (update-in [:pipes] (partial merge {id pipe}))))
    (throw (Exception. "not implemented"))))

(defmethod command->flowdef :default
           [command flowdef]
  (throw (Exception. (str "Command " (:type command) " not implemented yet for Cascading!"))))

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [commands]
  (let [flowdef (reduce (fn [def cmd] (command->flowdef cmd def)) {} commands)]
    flowdef))

