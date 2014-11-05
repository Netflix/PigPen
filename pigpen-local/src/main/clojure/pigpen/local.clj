(ns pigpen.local
  (:refer-clojure :exclude [load load-reader read])
  (:require [pigpen.oven :as oven]
            [clojure.java.io :as io]
            [pigpen.extensions.io :refer [list-files]]
            [pigpen.extensions.core :refer [forcat]])
  (:import [java.io Closeable]
           [java.io Writer]))

(defn cross-product [data]
  (if (empty? data) [{}]
    (let [head (first data)]
      (apply concat
        (for [child (cross-product (rest data))]
          (for [value head]
            (merge child value)))))))

(defn pigpen-compare [[key order & sort-keys] x y]
  (let [r (compare (key x) (key y))]
    (if (= r 0)
      (if sort-keys
        (recur sort-keys x y)
        (int 0))
      (case order
        :asc r
        :desc (int (- r))))))

(defn pigpen-comparator [sort-keys]
  (reify java.util.Comparator
    (compare [this x y]
      (pigpen-compare sort-keys x y))
    (equals [this obj]
      (= this obj))))

(defn ^:private eval-code [{:keys [init func]}]
  (eval init)
  (eval func))

(defmulti graph->local (fn [data command] (:type command)))

(defn graph->local+ [data {:keys [id ancestors] :as command}]
  ;(prn 'id id)
  (let [ancestor-data (mapv data ancestors)
        ;_ (prn 'ancestor-data ancestor-data)
        result (graph->local ancestor-data command)]
    ;(prn 'result result)
    (assoc data id result)))

(defn dump [query]
  (let [graph (oven/bake query :local {} {})
        last-command (:id (last graph))]
    (->> graph
      (reduce graph->local+ {})
      (last-command)
      (map 'value))))

;; ********** IO **********

(defmethod graph->local :return
  [_ {:keys [data]}]
  data)

; Override these to tweak how files are listed and read with the load loader.
; This is useful for reading from S3

(defmulti load-list (fn [location] (second (re-find #"^([a-z0-9]+)://" location))))

(defmethod load-list :default [location]
  (list-files location))

(defmulti load-reader (fn [location] (second (re-find #"^([a-z0-9]+)://" location))))

(defmethod load-reader :default [location]
  (io/reader location))

(defmulti store-writer (fn [location] (second (re-find #"^([a-z0-9]+)://" location))))

(defmethod store-writer :default [location]
  (io/writer location))

; Create one of these to provide a loader for another storage format, such as parquet

(defprotocol PigPenLocalLoader
  (locations [this])
  (init-reader [this file])
  (read [this reader])
  (close-reader [this reader]))

; Override this to register the custom storage format

(defmulti load
  "Defines a local implementation of a loader. Should return a PigPenLocalLoader."
  :storage)

(defmethod load :string [{:keys [location fields opts]}]
  {:pre [(= 1 (count fields))]}
  (reify PigPenLocalLoader
    (locations [_]
      (load-list location))
    (init-reader [_ file]
      (load-reader file))
    (read [_ reader]
      (for [line (line-seq reader)]
        {(first fields) line}))
    (close-reader [_ reader]
      (.close ^Closeable reader))))

; Create one of these to provide a storer for another storage format, such as parquet

(defprotocol PigPenLocalStorage
  (init-writer [this])
  (write [this writer value])
  (close-writer [this writer]))

; Override this to register the custom storage format

(defmulti store
  "Defines a local implementation of storage. Should return a PigPenLocalStorage."
  :storage)

(defmethod store :string [{:keys [location fields]}]
  {:pre [(= 1 (count fields))]}
  (reify PigPenLocalStorage
    (init-writer [_]
      (store-writer location))
    (write [_ writer value]
      (let [line (str ((first fields) value) "\n")]
        (.write ^Writer writer line)))
    (close-writer [_ writer]
      (.close ^Writer writer))))

; Uses the abstractions defined above to load the data

(defmethod graph->local :load
  [_ command]
  (let [local-loader (load command)]
    (vec
      (forcat [file (locations local-loader)]
        (let [reader (init-reader local-loader file)]
          (try
            (vec (read local-loader reader))
            (finally
              (close-reader local-loader reader))))))))

(defmethod graph->local :store
  [[data] command]
  (let [local-storage (store command)
        writer (init-writer local-storage)]
    (doseq [value data]
      (write local-storage writer value))
    (close-writer local-storage writer)
    data))

;; ********** Map **********

(defmethod graph->local :projection-field
  [values {:keys [field alias]}]
  (cond
    ; normal field names
    (symbol? field) [{alias (values field)}]
    ; compound field names (to be deprecated)
    (vector? field) [{alias (values field)}]
    ; used to select an index from a tuple output. Assumes a single field
    (number? field) [{alias (get-in (-> values vals first) [field])}]
    :else (throw (IllegalStateException. (str "Unknown field " field)))))

(defmethod graph->local :projection-flat
  [values {:keys [code alias] :as command}]
  (let [args (:args code)
        f (eval-code (:expr code))
        result (f (map values args))]
    (for [value' result]
      {alias value'})))

(defmethod graph->local :generate
  [[data] {:keys [projections]}]
  (mapcat
    (fn [values]
      (->> projections
        (map (partial graph->local values))
        (cross-product)
        (seq)))
    data))

(defmethod graph->local :rank
  [[data] _]
  (map-indexed
    (fn [i v]
      (assoc v '$0 i))
    data))

(defmethod graph->local :order
  [[data] {:keys [sort-keys]}]
  (sort (pigpen-comparator sort-keys) data))
