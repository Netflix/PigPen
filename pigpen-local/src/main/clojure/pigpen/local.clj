;;
;;
;;  Copyright 2013 Netflix, Inc.
;;
;;     Licensed under the Apache License, Version 2.0 (the "License");
;;     you may not use this file except in compliance with the License.
;;     You may obtain a copy of the License at
;;
;;         http://www.apache.org/licenses/LICENSE-2.0
;;
;;     Unless required by applicable law or agreed to in writing, software
;;     distributed under the License is distributed on an "AS IS" BASIS,
;;     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;     See the License for the specific language governing permissions and
;;     limitations under the License.
;;
;;

(ns pigpen.local
  (:refer-clojure :exclude [load load-reader read])
  (:require [pigpen.runtime]
            [pigpen.oven :as oven]
            [clojure.java.io :as io]
            [pigpen.extensions.io :refer [list-files]]
            [pigpen.extensions.core :refer [forcat zipv]])
  (:import [java.io Closeable]
           [java.io Writer]))

; For local mode, we want to differentiate between nils in the data and nils as
; the lack of existence of data. We convert nil values into a sentinel nil value
; that we see when grouping and joining values.

(defn induce-sentinel-nil [value]
  (or value ::nil))

(defn remove-sentinel-nil [value]
  (when-not (= value ::nil)
    value))

(defmethod pigpen.runtime/pre-process [:local :frozen]
  [_ _]
  (fn [args]
    (mapv remove-sentinel-nil args)))

(defmethod pigpen.runtime/post-process [:local :frozen]
  [_ _]
  (fn [args]
    (let [args (mapv induce-sentinel-nil args)]
      (if (next args)
        args
        (first args)))))

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

(defn eval-code [{:keys [init func]}]
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
        (cross-product)))
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

;; ********** Filter **********

(defmethod graph->local :limit
  [[data] {:keys [n]}]
  (take n data))

(defmethod graph->local :sample
  [[data] {:keys [p]}]
  (filter (fn [_] (< (rand) p)) data))

;; ********** Join **********

(defmethod graph->local :group
  [data {:keys [ancestors keys join-types fields]}]
  (->>
    ;; map
    (zipv [a ancestors
           [k] keys
           d data
           j join-types]
      (forcat [values d]
        ;; This selects all of the fields that are produced by this relation
        (for [[[r] v :as f] (next fields)
              :when (= r a)]
          {:field f
           ;; This changes a nil values into a relation specific nil value
           ;; TODO use a better sentinel value here
           :key (or (values k) (keyword (name a) "nil"))
           :values (values v)
           :required (= j :required)})))
    ;; shuffle
    (apply concat)
    ;; reduce
    (group-by :key)
    (map (fn [[key key-group]]
           (->> key-group
             (group-by :field)
             (map (fn [[field field-group]]
                    [field (map :values field-group)]))
             (into
               ;; Start with the group key. If it's a single value, flatten it.
               ;; Keywords are the fake nils we put in earlier
               {(first fields) (if (and (keyword? key) (= "nil" (name key))) nil key)}))))
    ; remove rows that were required, but are not present (inner joins)
    (filter (fn [value]
              (every? identity
                      (zipv [a ancestors
                             [k] keys
                             j join-types]
                        (or (= j :optional)
                            (contains? value [[a] k]))))))))

;; ********** Set **********

(defmethod graph->local :distinct
  [[data] _]
  (distinct data))

(defmethod graph->local :union
  [data _]
  (apply concat data))
