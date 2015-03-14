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

(ns pigpen.parquet
  "*** ALPHA - Subject to change ***

  Functions for reading and writing parquet data.

  See: http://parquet.incubator.apache.org/

  Note: These are currently only supported by the local, rx, and pig platforms
"
  (:require [pigpen.raw :as raw]
            [pigpen.parquet.core :as pq]))

(set! *warn-on-reflection* true)

;; These namespaces need to be loaded to register multimethods. They are loaded
;; in a try/catch becasue not all of them are always available.
(def ^:private known-impls
  ['pigpen.local.parquet
   'pigpen.pig.parquet])

(doseq [ns known-impls]
  (try
    (require ns)
    (println (str "Loaded " ns))
    (catch Exception e
      #_(prn e))))

; TODO get rid of schema
(defn load-parquet
  "*** ALPHA - Subject to change ***

Loads data from a parquet file. Returns data as maps with keywords matching the
parquet column names.

  Example:

    (pig-parquet/load-parquet \"input.pq\" {:x :int, :y :chararray})

  Note: `schema` must be a map of field name to pig type. This parameter will
        likely be removed in a future release.

  See also: https://github.com/apache/incubator-parquet-mr
"
  {:added "0.2.7"}
  [location schema]
  (let [fields (->> schema keys (mapv (comp symbol name)))
        pig-schema (pq/schema->pig-schema schema)
        {:keys [id] :as load} (raw/load$ location :parquet fields {:schema pig-schema})]
    (->> load
      (raw/bind$ [] '(pigpen.runtime/map->bind (pigpen.runtime/args->map pigpen.runtime/native->clojure))
                 {:args (clojure.core/mapcat (juxt str #(symbol (name id) (name %))) fields), :field-type-in :native}))))

(defn store-parquet
  "*** ALPHA - Subject to change ***

Stores data to a parquet file. The relation prior to this command must be a map
with keywords matching the parquet columns to be stored.

  Example:

    (pig-parquet/store-parquet \"output.pq\" {:x :int, :y :chararray} foo)

  Note: `schema` must be a map of field name to pig type. This parameter will
        likely be removed in a future release.

  See also: https://github.com/apache/incubator-parquet-mr
"
  {:added "0.2.7"}
  [location schema relation]
  (let [fields (mapv (comp symbol name) (keys schema))]
    (->> relation
      (raw/bind$ [] `(pigpen.runtime/keyword-field-selector->bind ~(mapv keyword fields))
                 {:field-type-out :native
                  :alias fields})
      (raw/store$ location :parquet {:schema schema}))))
