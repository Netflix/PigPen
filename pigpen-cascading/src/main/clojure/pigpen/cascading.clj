(ns pigpen.cascading
  "Contains the operators for generating cascading flows in PigPen"
  (:require [pigpen.cascading.core :as cascading]
            [pigpen.raw :as raw]
            [pigpen.cascading.oven :as oven])
  (:import [cascading.tap Tap]
           [cascading.flow.hadoop HadoopFlowConnector]))

;; ********** Flow **********

(defn generate-flow
  "Transforms the relation specified into a Cascading flow that is ready to be executed."
  ([query] (generate-flow (HadoopFlowConnector.) query))
  ([connector query]
    (->> query
      (oven/bake {})
      (cascading/commands->flow connector))))

;; ********** Customer loaders **********

(defn load-tap
  "This is a thin wrapper around a tap. By default a vector of
  the tap's source fields is created and returned as a single field.
  A custom function bind-fn can be provided to map the tap's
  source fields onto a single value in some other way."
  ([^Tap tap]
    (load-tap tap 'clojure.core/vector))
  ([^Tap tap bind-fn]
    (let [fields (mapv symbol (.getSourceFields tap))]
      (->>
        (raw/load$ (.toString tap) :tap fields {:tap tap})
        (raw/bind$
          `(pigpen.runtime/map->bind ~bind-fn)
          {:field-type-in :native})))))
