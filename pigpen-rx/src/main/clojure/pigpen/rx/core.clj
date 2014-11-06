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

(ns pigpen.rx.core
  (:require [pigpen.runtime]
            [pigpen.local :as local]
            [pigpen.oven :as oven]
            [clojure.java.io :as io]
            [rx.lang.clojure.core :as rx]
            [rx.lang.clojure.interop :as rx-interop]
            [rx.lang.clojure.blocking :as rx-blocking]
            [pigpen.rx.extensions :refer [multicast multicast->observable]]
            [pigpen.extensions.io :refer [list-files]]
            [pigpen.extensions.core :refer [forcat zipv]])
  (:import [rx Observable Observer Subscriber Subscription]
           [rx.schedulers Schedulers]
           [rx.observables GroupedObservable]))

(require '[pigpen.extensions.test :refer [debug]])

(defmethod pigpen.runtime/pre-process [:rx :frozen]
  [_ _]
  (fn [args]
    (mapv local/remove-sentinel-nil args)))

(defmethod pigpen.runtime/post-process [:rx :frozen]
  [_ _]
  (fn [args]
    (let [args (mapv local/induce-sentinel-nil args)]
      (if (next args)
        args
        (first args)))))

(defmulti graph->observable (fn [data command] (:type command)))

(defn graph->observable+ [data {:keys [id ancestors] :as command}]
  ;(prn 'id id)
  (let [ancestor-data (mapv data ancestors)
        ancestor-data (mapv multicast->observable ancestor-data)
        ;_ (prn 'ancestor-data ancestor-data)
        result (graph->observable ancestor-data command)]
    ;(prn 'result result)
    (assoc data id (multicast result))))

(defn dump [query]
  (let [graph (oven/bake query :rx {} {})
        last-command (:id (last graph))]
    (->> graph
      (reduce graph->observable+ {})
      (last-command)
      (multicast->observable)
      (rx-blocking/into [])
      (map 'value))))

;; ********** IO **********

(defmethod graph->observable :return
  [_ {:keys [data]}]
  (rx/seq->o data))

(defmethod graph->observable :load
  [_ {:keys [location], :as command}]
  (let [local-loader (local/load command)
        ^Observable o (->> (rx/observable*
                             (fn [^Subscriber s]
                               (future
                                 (try
                                   (println "Start reading from " location)
                                   (doseq [file (local/locations local-loader)
                                           :while (not (.isUnsubscribed s))]
                                     (let [reader (local/init-reader local-loader file)]
                                       (doseq [value (local/read local-loader reader)
                                               :while (not (.isUnsubscribed s))]
                                         (rx/on-next s value))
                                       (local/close-reader local-loader reader)))
                                   (rx/on-completed s)
                                   ;; TODO test this more. Errors seem to cause deadlocks
                                   (catch Throwable t (rx/on-error s t))))))
                        (rx/finally
                          (println "Stop reading from " location)))]
    (-> o
      (.onBackpressureBuffer)
      (.observeOn (Schedulers/io)))))

(defmethod graph->observable :store
  [[data] {:keys [location], :as command}]
  (let [local-storage (local/store command)
        writer (delay
                 (println "Start writing to " location)
                 (local/init-writer local-storage))]
    (->> data
      (rx/do (fn [value]
               (local/write local-storage @writer value)))
      (rx/finally
        (when (realized? writer)
          (println "Stop writing to " location)
          (local/close-writer local-storage @writer))))))

;; ********** Map **********

(defmethod graph->observable :generate
  [[data] {:keys [projections]}]
  (rx/flatmap
    (fn [values]
      (->> projections
        (map (partial local/graph->local values))
        (local/cross-product)
        (rx/seq->o)))
    data))

(defmethod graph->observable :rank
  [[data] _]
  (rx/map-indexed (fn [i v] (assoc v '$0 i)) data))

(defmethod graph->observable :order
  [[data] {:keys [sort-keys]}]
  (rx/sort (partial local/pigpen-compare sort-keys) data))

;; ********** Filter **********

(defmethod graph->observable :limit
  [[data] {:keys [n]}]
  (rx/take n data))

(defmethod graph->observable :sample
  [[data] {:keys [p]}]
  (rx/filter (fn [_] (< (rand) p)) data))

;; ********** Join **********

(defn graph->observable-group
  [data {:keys [ancestors keys join-types fields]}]
  (->>
    ;; map
    (zipv [a ancestors
           [k] keys
           d data
           j join-types]
      (->> d
        (rx/flatmap
          (fn [values]
            ;; This selects all of the fields that are produced by this relation
            (rx/seq->o
              (for [[[r] v :as f] (next fields)
                    :when (= r a)]
                {:field f
                 ;; This changes a nil values into a relation specific nil value
                 ;; TODO use a better sentinel value here
                 :key (or (values k) (keyword (name a) "nil"))
                 :values (values v)
                 :required (= j :required)}))))))
    ;; shuffle
    (apply rx/merge)
    ;; reduce
    (rx/group-by :key)
    (rx/flatmap (fn [[key key-group]]
                  (->> key-group
                    (rx/group-by :field)
                    (rx/flatmap (fn [[field field-group-o]]
                                  (->> field-group-o
                                    (rx/into [])
                                    (rx/map (fn [field-group]
                                              [field (map :values field-group)])))))
                    (rx/into
                      ;; Start with the group key. If it's a single value, flatten it.
                      ;; Keywords are the fake nils we put in earlier
                      {(first fields) (if (and (keyword? key) (= "nil" (name key))) nil key)}))))
    ; remove rows that were required, but are not present (inner joins)
    (rx/filter (fn [value]
                 (every? identity
                         (zipv [a ancestors
                                [k] keys
                                j join-types]
                           (or (= j :optional)
                               (contains? value [[a] k]))))))))

(defn graph->observable-group-all
  [data {:keys [fields]}]
  (->>
    (zipv [[[r] v :as f] (next fields)
           d data]
      (->> d
        (rx/map (fn [values]
                  {:field f
                   :values (v values)}))))
    (apply rx/merge)
    (rx/group-by :field)
    (rx/flatmap (fn [[field field-group-o]]
                  (->> field-group-o
                    (rx/into [])
                    (rx/map (fn [field-group]
                              [field (map :values field-group)])))))
    (rx/into {(first fields) nil})
    (rx/filter next)))

(defmethod graph->observable :group
  [data {:keys [keys] :as command}]
  (if (= keys [:pigpen.raw/group-all])
    (graph->observable-group-all data command)
    (graph->observable-group data command)))

(defmethod graph->observable :join
  [data {:keys [ancestors keys join-types fields]}]
  (->>
    (zipv [a ancestors
           [k] keys
           d data]
      (->> d
        (rx/map (fn [values]
                  ;; This selects all of the fields that are in this relation
                  {:values (into {} (for [[[r v] :as f] fields
                                          :when (= r a)]
                                      [f (values v)]))
                   ;; This is to emulate the way pig handles nils
                   ;; This changes a nil values into a relation specific nil value
                   :key (or (values k) (keyword (name a) "nil"))
                   :relation a}))))
    (apply rx/merge)
    (rx/group-by :key)
    (rx/flatmap (fn [[_ key-group]]
                  (->> key-group
                    (rx/group-by :relation)
                    (rx/flatmap (fn [[relation relation-grouping-o]]
                                  (->> relation-grouping-o
                                    (rx/into [])
                                    (rx/map (fn [relation-grouping]
                                              [relation (map :values relation-grouping)])))))
                    (rx/into (->>
                               ;; This seeds the inner/outer joins, by placing a
                               ;; defualt empty value for inner joins
                               (zipmap ancestors join-types)
                               (filter (fn [[_ j]] (= j :required)))
                               (map (fn [[a _]] [a []]))
                               (into {})))
                    (rx/flatmap (fn [relation-grouping]
                                  (rx/seq->o (local/cross-product (vals relation-grouping))))))))))

;; ********** Set **********

(defmethod graph->observable :distinct
  [[data] _]
  (rx/distinct data))

(defmethod graph->observable :union
  [data _]
  (apply rx/merge data))
