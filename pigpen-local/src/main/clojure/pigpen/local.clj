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
  (:require [schema.macros :as s] ;; TODO why doesn't schema.core load properly?
            [pigpen.model :as m]
            [pigpen.runtime]
            [pigpen.raw :as raw]
            [pigpen.oven :as oven]
            [clojure.java.io :as io]
            [pigpen.extensions.io :refer [list-files]]
            [pigpen.extensions.core :refer [forcat zipv]])
  (:import [java.io Closeable]
           [java.io Writer]))

(require '[pigpen.extensions.test :refer [debug]])

; For local mode, we want to differentiate between nils in the data and nils as
; the lack of existence of data. We convert nil values into a sentinel nil value
; that we see when grouping and joining values.

(defn induce-sentinel-nil [value]
  (or value ::nil))

(defn induce-sentinel-nil+ [value id]
  ;; TODO use a better sentinel value here
  (or value (keyword (name id) "nil")))

(defn remove-sentinel-nil [value]
  (when-not (= value ::nil)
    value))

(defn remove-sentinel-nil+ [value]
  (if (and (keyword? value) (= "nil" (name value)))
    nil
    value))

(defmethod pigpen.runtime/pre-process [:local :frozen]
  [_ _]
  (fn [args]
    (mapv remove-sentinel-nil args)))

(defmethod pigpen.runtime/post-process [:local :frozen]
  [_ _]
  (fn [args]
    (mapv induce-sentinel-nil args)))

(defn cross-product [data]
  (if (empty? data) [{}]
    (let [head (first data)]
      (apply concat
        (for [child (cross-product (rest data))]
          (for [value head]
            (merge child value)))))))

(defn update-field-ids [id]
  (fn [vs]
    (->> vs
      (map (fn [[f v]] [(raw/update-ns id f) v]))
      (into {}))))

(defmulti eval-func (fn [udf f args] udf))

(defmethod eval-func :sequence
  [_ f args]
  (f args))

(defmethod eval-func :algebraic
  [_ {:keys [pre combinef reducef post]} [values]]
  (->> values
    (mapv remove-sentinel-nil)
    pre
    (split-at (/ (count values) 2))
    (map (partial reduce reducef (combinef)))
    (reduce combinef)
    post
    vector))

(s/defn eval-code
  [{:keys [udf expr args]} :- m/Code
   values]
  (let [{:keys [init func]} expr
        _ (eval init)
        f (eval func)
        ;; TODO don't like - need to meditate on this one for a bit
        arg-values (map #(if (string? %) % (get values %)) args)
        result (eval-func udf f arg-values)]
    result))

(defmulti graph->local (fn [data command] (:type command)))

(defn graph->local+ [data {:keys [id ancestors fields] :as command}]
  ;(prn 'id id)
  (let [ancestor-data (mapv data ancestors)
        ;_ (prn 'ancestor-data ancestor-data)
        result (graph->local ancestor-data command)]
    #_(when (first result)
       (assert (= (set (keys (first result))) (set fields))
               (str "Field difference. Expecting " fields " Actual " (keys (first result)))))
    ;(prn 'result result)
    (assoc data id result)))

;; TODO add a version that returns a multiset
(defn dump
  "Executes a script locally and returns the resulting values as a clojure
sequence. This command is very useful for unit tests.

  Example:

    (->>
      (pig/load-clj \"input.clj\")
      (pig/map inc)
      (pig/filter even?)
      (pig/dump)
      (clojure.core/map #(* % %))
      (clojure.core/filter even?))

    (deftest test-script
      (is (= (->>
               (pig/load-clj \"input.clj\")
               (pig/map inc)
               (pig/filter even?)
               (pig/dump))
             [2 4 6])))

  Note: pig/store commands return an empty set
        pig/script commands merge their results

  See also: pigpen.core/show, pigpen.core/dump&show
"
  {:added "0.1.0"}
  ([query] (dump {} query))
  ([opts query]
    (let [graph (oven/bake query :local {} opts)
          last-command (:id (last graph))]
      (->> graph
        (reduce graph->local+ {})
        (last-command)
        (map (comp val first))))))

;; ********** IO **********

(s/defmethod graph->local :return
  [_ {:keys [data]} :- m/Return]
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

(s/defmethod load :string
  [{:keys [location fields opts]} :- m/Load]
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

(s/defmethod store :string
  [{:keys [location arg]} :- m/Store]
  (reify PigPenLocalStorage
    (init-writer [_]
      (store-writer location))
    (write [_ writer value]
      (let [line (str (get value arg) "\n")]
        (.write ^Writer writer line)))
    (close-writer [_ writer]
      (.close ^Writer writer))))

; Uses the abstractions defined above to load the data

(s/defmethod graph->local :load
  [_ command :- m/Load]
  (let [local-loader (load command)]
    (vec
      (forcat [file (locations local-loader)]
        (let [reader (init-reader local-loader file)]
          (try
            (vec (read local-loader reader))
            (finally
              (close-reader local-loader reader))))))))

(s/defmethod graph->local :store
  [[data] {:keys [id] :as command} :- m/Store]
  (let [local-storage (store command)
        writer (init-writer local-storage)]
    (doseq [value data]
      (write local-storage writer value))
    (close-writer local-storage writer)
    data))

;; ********** Map **********

(s/defmethod graph->local :projection-field
  [values {:keys [field alias]} :- m/ProjectionField]
  [{(first alias) (values field)}])

(s/defmethod graph->local :projection-func
  [values {:keys [code alias]} :- m/ProjectionFunc]
  [(zipmap alias (eval-code code values))])

(s/defmethod graph->local :projection-flat
  [values {:keys [code alias] :as command} :- m/ProjectionFlat]
  (for [value' (eval-code code values)]
    (zipmap alias value')))

(s/defmethod graph->local :generate
  [[data] {:keys [projections] :as c} :- m/Mapcat]
  (mapcat
    (fn [values]
      (->> projections
        (map (partial graph->local values))
        (cross-product)))
    data))

(s/defmethod graph->local :rank
  [[data] {:keys [id]} :- m/Rank]
  (->> data
    (map-indexed (fn [i v]
                   (assoc v 'index i)))
    (map (update-field-ids id))))

(s/defmethod graph->local :order
  [[data] {:keys [id key comp]} :- m/Sort]
  (->> data
    (sort-by key
             (case comp
               :asc compare
               :desc (clojure.core/comp - compare)))
    (map #(dissoc % key))
    (map (update-field-ids id))))

;; ********** Filter **********

(s/defmethod graph->local :limit
  [[data] {:keys [id n]} :- m/Take]
  (->> data
    (take n)
    (map (update-field-ids id))))

(s/defmethod graph->local :sample
  [[data] {:keys [id p]} :- m/Sample]
  (->> data
    (filter (fn [_] (< (rand) p)))
    (map (update-field-ids id))))

;; ********** Join **********

(s/defmethod graph->local :reduce
  [[data] {:keys [fields arg]} :- m/Reduce]
  (when (seq data)
    [{(first fields) (map arg data)}]))

(s/defmethod graph->local :group
  [data {:keys [ancestors keys join-types fields]} :- m/Group]
  (let [[group-field & data-fields] fields
        join-types (zipmap keys join-types)]
    (->>
      ;; map
      (zipv [d data
             id ancestors
             k keys]
        (for [values d
              :let [key (induce-sentinel-nil+ (values k) id)]
              [f v] values]
          {;; This changes a nil values into a relation specific nil value
           :field f
           :key key
           :value v}))
      ;; shuffle
      (apply concat)
      (group-by :key)
      ;; reduce
      (map (fn [[key key-group]]
             (->> key-group
               (group-by :field)
               (map (fn [[field field-group]]
                      [field (map :value field-group)]))
               (into
                 ;; Revert the fake nils we put in the key earlier
                 {group-field (remove-sentinel-nil+ key)}))))
      ; remove rows that were required, but are not present (inner joins)
      (remove (fn [value]
                (->> join-types
                  (some (fn [[k j]]
                          (and (= j :required)
                               (not (contains? value k)))))))))))

(s/defmethod graph->local :join
  [data {:keys [ancestors keys join-types fields]} :- m/Join]
  (let [join-types (zipmap ancestors join-types)
        ;; This seeds the inner/outer joins, by placing a
        ;; defualt empty value for inner joins
        seed-value (->> join-types
                     (filter (fn [[_ j]] (= j :required)))
                     (map (fn [[a _]] [a []]))
                     (into {}))]
    (->>
      ;; map
      (zipv [d data
             id ancestors
             k keys]
        (for [values d]
          {:relation id
           ;; This changes a nil values into a relation specific nil value
           :key (induce-sentinel-nil+ (values k) id)
           :values values}))
      ;; shuffle
      (apply concat)
      (group-by :key)
      ;; reduce
      (mapcat (fn [[_ key-group]]
                (->> key-group
                  (group-by :relation)
                  (map (fn [[relation relation-grouping]]
                         [relation (map :values relation-grouping)]))
                  (into seed-value)
                  vals
                  cross-product))))))

;; ********** Set **********

(s/defmethod graph->local :distinct
  [[data] {:keys [id]} :- m/Distinct]
  (->> data
    (distinct)
    (map (update-field-ids id))))

(s/defmethod graph->local :union
  [data {:keys [id]} :- m/Concat]
  (->> data
    (apply concat)
    (map (update-field-ids id))))

;; ********** Script **********

(s/defmethod graph->local :noop
  [[data] {:keys [id]} :- m/NoOp]
  (map (update-field-ids id) data))

(s/defmethod graph->local :script
  [data _]
  (apply concat data))
