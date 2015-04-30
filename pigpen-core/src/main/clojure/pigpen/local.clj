;;
;;
;;  Copyright 2013-2015 Netflix, Inc.
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
  (:require [schema.core :as s]
            [pigpen.model :as m]
            [pigpen.runtime]
            [pigpen.raw :as raw]
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
  (if (nil? value)
    ::nil
    value))

(defn induce-sentinel-nil+ [value id]
  ;; TODO use a better sentinel value here
  (if (nil? value)
    (keyword (name id) "nil")
    value))

(defn remove-sentinel-nil [value]
  (when-not (= value ::nil)
    value))

(defn remove-sentinel-nil+ [value]
  (when-not (and (keyword? value) (= "nil" (name value)))
    value))

(defn pre-process [args]
  (mapv remove-sentinel-nil args))

(defn post-process [args]
  (mapv induce-sentinel-nil args))

(defmethod pigpen.runtime/pre-process [:local :frozen]
  [_ _]
  pre-process)

(defmethod pigpen.runtime/post-process [:local :frozen]
  [_ _]
  post-process)

(defn cross-product [[head & more]]
  (if head
    (for [value head
          child (cross-product more)]
      (merge child value))
    [{}]))

(defn pigpen-comparator [comp]
  (case comp
    :asc compare
    :desc (clojure.core/comp - compare)))

(defn update-field-ids [id]
  (fn [vs]
    (->> vs
      (map (fn [[f v]] [(raw/update-ns id f) v]))
      (into {}))))

(defmulti eval-func (fn [udf f args] udf))

(defmethod eval-func :seq
  [_ xf args]
  ((xf conj) [] args))

(defmethod eval-func :fold
  [_ {:keys [pre combinef reducef post]} [values]]
  (->> values
    (mapv remove-sentinel-nil)
    pre
    (split-at (/ (count values) 2))
    (map (partial reduce reducef (combinef)))
    (reduce combinef)
    post
    vector))

(defmulti eval-expr
  (fn [state expr values]
    (:type expr)))

(s/defmethod eval-expr :field
  [state
   {:keys [field]} :- m/FieldExpr
   values]
  [(values field)])

(defn field-lookup [values arg]
  (cond
    (string? arg) arg
    (symbol? arg) (get values arg)
    :else (throw (ex-info "Unknown arg" {:arg arg, :values values}))))

(defn eval-user-code [state f]
  (let [cache (:code-cache state)]
    (if-let [e (find @cache f)]
      (val e)
      (let [ret (eval f)]
        (swap! cache assoc f ret)
        ret))))

(s/defmethod eval-expr :code
  [state
   {:keys [udf init func args]} :- m/CodeExpr
   values]
  (eval-user-code state init)
  (let [f (eval-user-code state func)
        arg-values (map (partial field-lookup values) args)]
    (eval-func udf f arg-values)))

(defmulti graph->local (fn [state data command] (:type command)))

(defn graph->local+ [state data {:keys [id ancestors fields] :as command}]
  ;(prn 'id id)
  (let [ancestor-data (mapv data ancestors)
        ;_ (prn 'ancestor-data ancestor-data)
        result (vec (graph->local state ancestor-data command))]
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

  Note: pig/store commands return the output data
        pig/store-many commands merge their results

  Note: The original rx pigpen.core/dump command is now pigpen.rx/dump. This
        implementation uses lazy seqs instead.
"
  {:added "0.3.0"}
  ([query] (dump {} query))
  ([opts query]
    (let [state {:code-cache (atom {})}
          graph (oven/bake :local {} opts query)
          last-command (:id (last graph))]
      (->> graph
        (reduce (partial graph->local+ state) {})
        (last-command)
        (map (comp remove-sentinel-nil val first))))))

;; ********** IO **********

(s/defmethod graph->local :return
  [_ _ {:keys [data]} :- m/Return]
  data)

; Override these to tweak how files are listed and read with the load loader.
; This is useful for reading from S3

(defmulti load-list (fn [location] (second (re-find #"^([a-z0-9]+)://" location))))

(defmethod load-list :default [location]
  (list-files location))

(defmulti load-reader (fn [location] (second (re-find #"^([a-z0-9]+)://" location))))

(defmethod load-reader :default [^String location]
  (if (.endsWith location ".gz")
    (io/reader (java.util.zip.GZIPInputStream. (io/input-stream location)))
    (io/reader (io/input-stream location))))

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
  [{:keys [location args]} :- m/Store]
  (reify PigPenLocalStorage
    (init-writer [_]
      (store-writer location))
    (write [_ writer value]
      (let [line (str (get value (first args)) "\n")]
        (.write ^Writer writer line)))
    (close-writer [_ writer]
      (.close ^Writer writer))))

; Uses the abstractions defined above to load the data

(s/defmethod graph->local :load
  [_ _ command :- m/Load]
  (let [local-loader (load command)]
    (vec
      (forcat [file (locations local-loader)]
        (let [reader (init-reader local-loader file)]
          (try
            (vec (read local-loader reader))
            (finally
              (close-reader local-loader reader))))))))

(s/defmethod graph->local :store
  [_ [data] {:keys [id] :as command} :- m/Store]
  (let [local-storage (store command)
        writer (init-writer local-storage)]
    (doseq [value data]
      (write local-storage writer value))
    (close-writer local-storage writer)
    data))

;; ********** Map **********

(s/defmethod graph->local :projection
  [state values {:keys [expr flatten alias]} :- m/Projection]
  (let [result (eval-expr state expr values)]
    (if flatten
      (map (partial zipmap alias) result)
      (zipmap alias result))))

(s/defmethod graph->local :project
  [state [data] {:keys [projections] :as c} :- m/Project]
  (mapcat
    (fn [values]
      (->> projections
        (map (partial graph->local state values))
        (cross-product)))
    data))

(s/defmethod graph->local :rank
  [_ [data] {:keys [id]} :- m/Rank]
  (->> data
    (map-indexed (fn [i v]
                   (assoc v 'index i)))
    (map (update-field-ids id))))

(s/defmethod graph->local :sort
  [_ [data] {:keys [id key comp]} :- m/Sort]
  (->> data
    (sort-by key (pigpen-comparator comp))
    (map #(dissoc % key))
    (map (update-field-ids id))))

;; ********** Filter **********

(defn filter-expr->fn [id expr]
  (let [keys  (atom #{})
        ;; find all symbols starting with ? and create a destructuring form
        _ (clojure.walk/postwalk
            (fn [x]
              (when (and (symbol? x)
                         (.startsWith (str x) "?"))
                (swap! keys conj x)
                x))
            expr)
        keys' (->> @keys
                (map (fn [x] `[~x '~(symbol (name id) (subs (name x) 1))]))
                (into {}))]
    (eval
      `(fn [~keys']
         ~expr))))

(s/defmethod graph->local :filter
  [_ [data] {:keys [id expr]} :- m/Filter]
  (->> data
    (map (update-field-ids id))
    (filter (filter-expr->fn id expr))))

(s/defmethod graph->local :take
  [_ [data] {:keys [id n]} :- m/Take]
  (->> data
    (take n)
    (map (update-field-ids id))))

(s/defmethod graph->local :sample
  [_ [data] {:keys [id p]} :- m/Sample]
  (->> data
    (filter (fn [_] (< (rand) p)))
    (map (update-field-ids id))))

;; ********** Join **********

(s/defmethod graph->local :reduce
  [_ [data] {:keys [fields arg]} :- m/Reduce]
  (when (seq data)
    [{(first fields) (map arg data)}]))

(s/defmethod graph->local :group
  [_ data {:keys [ancestors keys join-types fields]} :- m/Group]
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

(defn join-seed-value [ancestors join-types]
  ;; This seeds the inner/outer joins, by placing a
  ;; defualt empty value for inner joins
  (->>
    (zipmap ancestors join-types)
    (filter (fn [[_ j]] (= j :required)))
    (map (fn [[a _]] [a []]))
    (into {})))

(s/defmethod graph->local :join
  [_ data {:keys [ancestors keys join-types fields]} :- m/Join]
  (let [seed-value (join-seed-value ancestors join-types)]
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
  [_ [data] {:keys [id]} :- m/Distinct]
  (->> data
    (distinct)
    (map (update-field-ids id))))

(s/defmethod graph->local :concat
  [_ data {:keys [id]} :- m/Concat]
  (->> data
    (apply concat)
    (map (update-field-ids id))))

;; ********** Script **********

(s/defmethod graph->local :noop
  [_ [data] {:keys [id]} :- m/NoOp]
  (map (update-field-ids id) data))

(s/defmethod graph->local :store-many
  [_ data _]
  (apply concat data))
