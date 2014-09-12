(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine)
           (cascading.pipe Pipe)
           (cascading.flow.hadoop HadoopFlowConnector))
  (:require [pigpen.raw :as raw]))

(defn- get-tap-fn [name]
  (let [tap ({"text" (fn [location] (Hfs. (TextLine.) location))}
             name)]
    (if (nil? tap)
      (throw (Exception. (str "Unrecognized tap type: " name)))
      tap)))

(defn load-text [location]
  (-> (raw/load$ location '[offset line] (raw/storage$ [] "text" {}) {})
      (raw/bind$
        '(pigpen.pig/map->bind (fn [_ line] line))
        {:args '[offset line]
         :field-type-in :native})))

(defn store-text [location relation]
  (raw/store$ relation location (raw/storage$ [] "text" {}) {}))

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
  (-> flowdef
      (update-in [:pipe-to-sink] (partial merge {(first ancestors) id}))
      (update-in [:sinks] (partial merge {id ((get-tap-fn (:func storage)) location)}))))

(defmethod command->flowdef :generate
           [{:keys [id ancestors projections opts]} flowdef]
  {:pre [id ancestors (not-empty projections)]}
  (if (contains? (:sources flowdef) (first ancestors))
    (let [pipe (Pipe. (str id))]
      (-> flowdef
          (update-in [:pipe-to-source] (partial merge {id (first ancestors)}))
          (update-in [:pipes] (partial merge {id pipe}))))
    (throw (Exception. "not implemented"))))

(defmethod command->flowdef :default
           [command flowdef]
  (throw (Exception. (str "Command " (:type command) " not implemented yet for Cascading!"))))

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [commands]
  (let [flowdef (reduce (fn [def cmd] (command->flowdef cmd def)) {} commands)
        {:keys [pipe-to-source sources pipe-to-sink sinks pipes]} flowdef
        sources-map (into {} (map (fn [[p s]] [(str p) (sources s)]) pipe-to-source))
        sinks-map (into {} (map (fn [[p s]] [(str p) (sinks s)]) pipe-to-sink))
        tail-pipes (into-array Pipe (map #(pipes %) (keys pipe-to-sink)))]
    (println sources-map)
    (println sinks-map)
    (println tail-pipes)
    (.connect (HadoopFlowConnector.) sources-map sinks-map tail-pipes)))

