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
  "Contains functions for running PigPen locally.

Nothing in here will be used directly with normal PigPen usage.
See pigpen.core and pigpen.exec
"
  (:refer-clojure :exclude [load load-reader read])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [pigpen.rx.extensions.core :refer [multicast]]
            [pigpen.extensions.io :refer [list-files]]
            [pigpen.pig :as pig])
  (:import [pigpen PigPenException]
           [org.apache.pig EvalFunc]
           [org.apache.pig.data Tuple DataBag DataByteArray]
           [rx Observable Observer Subscription]
           [rx.concurrency Schedulers]
           [rx.observables GroupedObservable BlockingObservable]
           [rx.util.functions Action0]
           [java.util.regex Pattern]
           [java.io Reader Writer]
           [java.util List]
           [java.io Closeable]))

(set! *warn-on-reflection* true)

(defn dereference
  "Pig handles tuples implicitly. This gets the first value if the field is a tuple."
  ([value] (dereference value 0))
  ([value index]
    (if (instance? Tuple value)
      (.get ^Tuple value index)
      value)))

(def ^:dynamic udf-scope nil)

(def create-udf
  (memoize
    (fn ^EvalFunc [scope return init func]
      (eval `(new ~(symbol (str "pigpen.PigPenFn" return)) ~(str init) ~(str func))))))

;; TODO add option to skip this for faster execution
(defn ^:private eval-code [{:keys [return expr args]} values]
  (let [{:keys [init func]} expr
        ^EvalFunc instance (create-udf udf-scope return init func)
        ^Tuple tuple (->> args
                       (map #(if ((some-fn symbol? vector?) %) (dereference (values %)) %))
                       (apply pig/tuple))]
    (try
      (.exec instance tuple)
      (catch PigPenException z (throw (.getCause z))))))

(defn ^:private cross-product [data]
  (if (empty? data) [{}]
    (let [head (first data)]
      (apply concat
        (for [child (cross-product (rest data))]
          (for [value head]
            (merge child value)))))))

;; TODO use the script options for this
(def debug false)

(defmulti graph->local (fn [command data] (:type command)))

(defn graph->local* [{:keys [id] :as command} data]
  (let [first (atom true)
        data' (for [^Observable d data]
                (.map d (fn [v]
                          (when @first
                            (println id "start")
                            (reset! first false))
                          v)))
        ^Observable o (graph->local command data')]
    (-> o
      (.finallyDo
        (reify Action0
          (call [this] (println id "stop")))))))

(defn ^:private find-next-o [observable-lookup]
  (fn [command]
    {:pre [(map? command)]}
    (let [ancestors (map observable-lookup (:ancestors command))]
      (if (every? (comp not nil?) ancestors)
        (let [^Observable o ((if debug graph->local* graph->local) command (map #(%) ancestors))]
          [(:id command) (multicast o (if debug (:id command)))])))))

(defn graph->observable
  ([commands]
    {:pre [(sequential? commands)]}
    (binding [udf-scope (gensym)]
      (let [observable (graph->observable (->> commands
                                            (map (juxt :id identity))
                                            (into {}))
                                          [])]
        (observable))))
  ([command-lookup observables]
    (if (empty? command-lookup) (second (last observables))
      (let [[id _ :as o] (some (find-next-o (into {} observables)) (vals command-lookup))]
        (recur (dissoc command-lookup id) (conj observables o))))))

(defn dereference-all ^Observable [^Observable o]
  (.map o #(->> %
             (map (fn [[k v]] [k (dereference v)]))
             (into {}))))

(defn observable->clj [^Observable o]
  (-> o
    BlockingObservable/toIterable
    seq
    vec))

(defn observable->data
  ^Observable [^Observable o]
  (-> o
    (dereference-all)
    (.map (comp 'value pig/thaw-values))
    observable->clj))

(defn observable->raw-data
  ^Observable [^Observable o]
  (-> o
    (dereference-all)
    (.map pig/thaw-anything)
    observable->clj))

;; ********** Util **********

(defmethod graph->local :register [_ _]
  (Observable/just nil))

(defmethod graph->local :option [_ _]
  (Observable/just nil))

;; ********** IO **********

(defmulti load
  "Defines a local implementation of a loader. Should return a PigPenLocalLoader."
  (fn [command] (get-in command [:storage :func])))

(defprotocol PigPenLocalLoader
  (locations [this])
  (init-reader [this file])
  (read [this reader])
  (close-reader [this reader]))

(defmulti store
  "Defines a local implementation of storage. Should return a PigPenLocalStorage."
  (fn [command] (get-in command [:storage :func])))

(defprotocol PigPenLocalStorage
  (init-writer [this])
  (write [this writer value])
  (close-writer [this writer]))

;; unsubscribe doesn't seem to work with this version of rx
;; This caps the maximum number of records read from a file/stream
(def ^:dynamic *max-load-records* 1000)

(defmethod graph->local :load
  [{:keys [location], :as command} _]
  (let [local-loader (load command)]
    (->
      (Observable/create
        (fn [^Observer o]
          (let [cancel (atom false)
                read-count (atom 0)]
            (future
              (try
                (println "Start reading from " location)
                (doseq [file (locations local-loader)
                        :while (not @cancel)
                        :while (< @read-count *max-load-records*)]
                  (let [reader (init-reader local-loader file)]
                    (doseq [value (read local-loader reader)
                            :while (not @cancel)
                            :while (< @read-count *max-load-records*)]
                      (swap! read-count inc)
                      (.onNext o value))
                    (close-reader local-loader reader)))
                (.onCompleted o)
                (catch Exception e (.onError o e))))
            (reify Subscription
              (unsubscribe [this]
                (reset! cancel true))))))
      (.finallyDo
        (reify Action0
          (call [this] (println "Stop reading from " location))))
      (.observeOn (Schedulers/threadPoolForIO)))))

(defmethod graph->local :store
  [{:keys [location], :as command} data]
  (let [local-storage (store command)
        writer (delay
                 (println "Start writing to " location)
                 (init-writer local-storage))]
    (-> data
      first
      (dereference-all)
      (.map
        (fn [value]
          (write local-storage @writer value)
          value))
      (.finallyDo
        (reify Action0
          (call [this] (when (realized? writer)
                         (println "Stop writing to " location)
                         (close-writer local-storage @writer))))))))

(defmethod graph->local :return [{:keys [^Iterable data]} _]
  (Observable/from data))

(defmulti load-list (fn [location] (re-find #"^([a-z0-9]+)://" location)))

(defmethod load-list :default [location]
  (list-files location))

(defmulti load-reader (fn [location] (re-find #"^([a-z0-9]+)://" location)))

(defmethod load-reader :default [location]
  (io/reader location))

(defmethod load "PigStorage" [{:keys [location fields storage opts]}]
  (let [{:keys [cast]} opts
        delimiter (-> storage :args first edn/read-string)
        delimiter (if delimiter (Pattern/compile (str delimiter)) #"\t")]
    (reify PigPenLocalLoader
      (locations [_]
        (load-list location))
      (init-reader [_ file]
        (load-reader location))
      (read [_ reader]
        (for [line (line-seq reader)]
          (->>
            (clojure.string/split line delimiter)
            (map (fn [^String s] (pig/cast-bytes cast (.getBytes s))))
            (zipmap fields))))
      (close-reader [_ reader]
        (.close ^Closeable reader)))))

(defmethod store "PigStorage" [{:keys [location fields]}]
  (reify PigPenLocalStorage
    (init-writer [_]
      (io/writer location))
    (write [_ writer value]
      (let [line (str (clojure.string/join "\t" (for [f fields] (str (f value)))) "\n")]
        (.write ^Writer writer line)))
    (close-writer [_ writer]
      (.close ^Writer writer))))

;; ********** Map **********

(defmethod graph->local :projection-field [{:keys [field alias]} values]
  (cond
    (symbol? field) [{alias (field values)}]
    (vector? field) [{alias (values field)}]
    (number? field) [{alias (dereference (first (vals values)) field)}]
    :else (throw (IllegalStateException. (str "Unknown field " field)))))

(defmethod graph->local :projection-func [{:keys [code alias]} values]
  [{alias (eval-code code values)}])

(defmethod graph->local :projection-flat [{:keys [code alias]} values]
  (let [result (eval-code code values)]
    (cond
      ;; Following Pig logic, Bags are actually flattened
      (instance? DataBag result) (for [v' result]
                                   {alias v'})
      ;; While Tuples just expand their elements
      (instance? Tuple result) (->>
                                 (.getAll ^Tuple result)
                                 (map-indexed vector)
                                 (into {}))
      :else (throw (IllegalStateException.
                     (str "Don't know how to flatten a " (type result)))))))
  
(defmethod graph->local :generate [{:keys [projections] :as command} data]
  (let [udf-scope' udf-scope ; yes this is ugly, but passing this as an explicit parameter breaks a lot of other code
        ^Observable data (first data)]
    (.mapMany data
      (fn [values]
        (let [^Iterable result (->> projections
                                 (map (fn [p]
                                        (binding [udf-scope udf-scope']
                                          (graph->local p values))))
                                 (cross-product))]
          (Observable/from result))))))

(defn ^:private pig-compare [[key order & sort-keys] x y]
  (let [r (compare (key x) (key y))]
    (if (= r 0)
      (if sort-keys
        (recur sort-keys x y)
        (int 0))
      (case order
        :asc r
        :desc (int (- r))))))

(defmethod graph->local :order [{:keys [sort-keys]} data]
  (let [^Observable data (first data)]
    (-> data
      (.toSortedList (partial pig-compare sort-keys))
      (.mapMany #(Observable/from ^Iterable %)))))

(defmethod graph->local :rank [{:keys [sort-keys]} data]
  (let [^Observable data (first data)]
    (if (not-empty sort-keys)
      (-> data
        (.toSortedList (partial pig-compare sort-keys))
        (.mapMany #(Observable/from ^Iterable (map-indexed (fn [i v] (assoc v '$0 i)) %))))
      (let [i (atom -1)]
        (-> data
          (.map (fn [v] (assoc v '$0 (swap! i inc)))))))))

;; ********** Filter **********

(defmethod graph->local :filter [{:keys [code]} data]
  (let [^Observable data (first data)]
    (.filter data
      (fn [values] (eval-code code values)))))

(defmethod graph->local :filter-native [{:keys [fields expr]} data]
  (if-not expr (first data)
    (let [^Observable data (first data)
          f (eval `(fn [{:syms ~fields}] ~expr))]
      (.filter data
        (fn [values] (f values))))))

(defmethod graph->local :distinct [_ data]
  (let [^Observable data (first data)
        seen (atom [false #{}])]
    (.filter data
      (fn [values]
        (let [[c _] (swap! seen (fn [[_ s]] [(contains? s values)
                                             (conj s values)]))]
          (not c))))))

(defmethod graph->local :limit [{:keys [n]} data]
  (let [^Observable data (first data)]
    (.take data n)))

(defmethod graph->local :sample [{:keys [p]} data]
  (let [^Observable data (first data)]
    (.filter data
      (fn [values] (< (rand) p)))))

;; ********** Join **********

(defmethod graph->local :union [_ ^List data]
  (Observable/merge data))

(defn ^:private graph->local-group [{:keys [ancestors keys join-types fields]} data]
  (->
    ^List
    (mapv (fn [a k ^Observable d j]
            (.mapMany d (fn [values]
                          ;; This selects all of the fields that are in this relation
                          (let [^Iterable result
                                (for [[[r] v :as f] (next fields)
                                      :when (= r a)]
                                  {:values (pig/tuple (values v))
                                   ;; This is to emulate the way pig handles nils
                                   ;; This changes a nil values into a relation specific nil value
                                   :key (mapv #(or (values %) (keyword (name a) "nil")) k)
                                   :relation f
                                   :required (= j :required)})]
                            (Observable/from result)))))
         ancestors keys data join-types)
    (Observable/merge)
    (.groupBy :key)
    (.mapMany (fn [^Observable o]
                (-> o
                  (.groupBy :relation)
                  (.mapMany #(-> ^Observable %
                               (.toList)
                               (.map (fn [v] [(if (keyword? (.getKey ^GroupedObservable %)) nil (.getKey ^GroupedObservable %))
                                              (apply pig/bag (mapv :values v))]))))
                  ;; Start with the group key. If it's a single value, flatten it.
                  ;; Keywords are the fake nils we put in earlier
                  (.reduce {(first fields) (let [k (.getKey ^GroupedObservable o)
                                                 k (if (= 1 (count k)) (first k) k)
                                                 k (if (keyword? k) nil k)]
                                             k)}
                    (fn [values [k v]]
                      (assoc values k v))))))
    ;; TODO This is a bad way to do inner groupings
    (.filter (fn [g]
               (every? identity
                 (map (fn [a [k] j] (or (= j :optional) (contains? g [[a] k]))) ancestors keys join-types))))))

(defn ^:private graph->local-group-all [{:keys [fields]} data]
  (->
    ^List
    (mapv (fn [[r v] ^Observable d]
            (.map d (fn [values]
                      ;; TODO clean up pig dereferencing
                      (let [v' (v values)]
                        {:values (if (instance? Tuple v') v' (pig/tuple v'))
                         :relation [r v]}))))
         (next fields) data)
    (Observable/merge)
    (.groupBy :relation)
    (.mapMany #(-> ^Observable %
                 (.toList)
                 (.map (fn [v] [(.getKey ^GroupedObservable %) (apply pig/bag (mapv :values v))]))))
    (.reduce {(first fields) nil}
      (fn [values [k v]]
        (assoc values k v)))
    (.filter (fn [v] (< 1 (count v))))))

(defmethod graph->local :group [{:keys [keys] :as command} data]
  (if (= keys [:pigpen.raw/group-all])
    (graph->local-group-all command data)
    (graph->local-group command data)))

(defmethod graph->local :join [{:keys [ancestors keys join-types fields]} data]
  (->
    ^List
    (mapv (fn [a k ^Observable d]
            (.map d (fn [values]
                      ;; This selects all of the fields that are in this relation
                      {:values (into {} (for [[[r v] :as f] fields
                                              :when (= r a)]
                                          [f (values v)]))
                       ;; This is to emulate the way pig handles nils
                       ;; This changes a nil values into a relation specific nil value
                       :key (mapv #(or (values %) (keyword (name a) "nil")) k)
                       :relation a})))
         ancestors keys data)
    (Observable/merge)
    (.groupBy :key)
    (.mapMany (fn [^Observable key-grouping]
                (-> key-grouping
                  (.groupBy :relation)
                  (.mapMany #(-> ^Observable %
                               (.toList)
                               (.map (fn [v] [(.getKey ^GroupedObservable %) (map :values v)]))))
                  (.reduce (->>
                             ;; This seeds the inner/outer joins, by placing a
                             ;; defualt empty value for inner joins
                             (zipmap ancestors join-types)
                             (filter (fn [[_ j]] (= j :required)))
                             (map (fn [[a _]] [a []]))
                             (into {}))
                    (fn [values [k v]]
                      (assoc values k v)))
                  (.mapMany (fn [relation-grouping]
                              (Observable/from ^Iterable (cross-product (vals relation-grouping))))))))))

;; ********** Script **********

(defmethod graph->local :script [_ data]
  (Observable/merge ^List (vec data)))
