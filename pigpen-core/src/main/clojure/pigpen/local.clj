(ns pigpen.local
  (:require [pigpen.oven :as oven]
            [pigpen.core :as pig]))

(defn ^:private cross-product [data]
  ;(prn "Applying cross-product over" data)
  (if (empty? data) [{}]
    (let [head (first data)]
      (apply concat
        (for [child (cross-product (rest data))]
          (for [value head]
            (merge child value)))))))

(defn ^:private pig-compare [[key order & sort-keys] x y]
  (let [r (compare (key x) (key y))]
    (if (= r 0)
      (if sort-keys
        (recur sort-keys x y)
        (int 0))
      (case order
        :asc r
        :desc (int (- r))))))

(defn ^:private eval-code [{:keys [init func]}]
  (eval init)
  (eval func))

;(pigpen.runtime/exec 
;  [(pigpen.runtime/process->bind (pigpen.runtime/pre-process :local :frozen)) 
;   (pigpen.runtime/map->bind (pigpen.runtime/with-ns pigpen-sandbox.core inc)) 
;   (pigpen.runtime/process->bind (pigpen.runtime/post-process :local :frozen))])

(defmulti graph->local (fn [data command] (:type command)))

(defmethod graph->local :register
  [data command]
  data)

(defmethod graph->local :return [_ {:keys [id data]}]
  {id data})

(defmethod graph->local :generate [data {:keys [ancestors projections id] :as command}]
  ;Add the preconditions here
  (let [ret (assoc data id
                   ;Extract the following in a common func
                   (let [ancestor (first ancestors)
                         ancestor-data (data ancestor)]
                     (mapcat
                       (fn [values]
                         (->> projections
                           (map (fn [p]
                                  (graph->local values p)))
                           (cross-product)
                           (seq)))
                       ancestor-data)))]
  ret))

(defmethod graph->local :projection-flat [data {:keys [code alias] :as command}]
  (let [args (:args code) 
        f (eval-code (:expr code))
        result (f (map data args))]
    (for [value' result] 
      {alias value'})))

(defmethod graph->local :projection-field [data {:keys [field alias] :as command}]
  (let [p-field (cond
                  (symbol? field) [{alias (field data)}]
                  (vector? field) [{alias (data field)}]
                  (number? field) [{alias {(first (vals data)) field}}]
                  :else (throw (IllegalStateException. (str "Unknown field " field))))]
    p-field))

(defmethod graph->local :rank [data {:keys [ancestors sort-keys id] :as command}]
  (let [ancestor-data (data (first ancestors))]
    (let [rank-ret (assoc data id
                          (let [rank (->> ancestor-data
                                       ;(not-empty sort-keys) (sort sort-keys)
                                       (map-indexed (fn [i v]
                                                      (assoc v '$0 i))))]
                            rank))]
      rank-ret)))

(defn dump [query]
  (let [graph (oven/bake :local query)
        last-command (:id (last graph))]
    (->> graph
      (reduce graph->local {})
      (last-command)
      (map 'value))))



