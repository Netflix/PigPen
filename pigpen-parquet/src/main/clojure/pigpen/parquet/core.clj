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

(ns pigpen.parquet.core
  (:require [pigpen.raw :as raw]
            [pigpen.pig-rx]
            [pigpen.script]
            [pigpen.hadoop.local :as hadoop]
            [pigpen.pig.local :as pig-local])
  (:import [parquet.pig ParquetLoader ParquetStorer]
           [pigpen.pig.local LoadFuncLoader StoreFuncStorage]))

(set! *warn-on-reflection* true)

(defn ^:private schema->pig-schema
  ([schema] (schema->pig-schema (keys schema) schema))
  ([fields schema]
    (->> fields
      (map (fn [field]
             (when-let [type (schema (keyword field))]
               (str (name field) ":" (name type)))))
      (filter identity)
      (clojure.string/join ","))))

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
        pig-schema (schema->pig-schema schema)]
    (-> location
      (raw/load$ fields :parquet {:schema pig-schema})
      (raw/bind$ [] '(pigpen.runtime/map->bind (pigpen.runtime/args->map pigpen.pig/native->clojure))
                 {:args (clojure.core/mapcat (juxt str identity) fields), :field-type-in :native}))))

(defmethod pigpen.pig-rx/load :parquet
  [{:keys [location fields storage]}]
  (let [schema (first (:args storage))]
    (LoadFuncLoader. (ParquetLoader. schema) {} location fields)))

(defmethod pigpen.script/storage->script [:load :parquet]
  [command]
  (str "parquet.pig.ParquetLoader(" (get-in command [:opts :schema]) ")"))

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
  (let [fields (map (comp symbol name) (keys schema))]
    (-> relation
      (raw/bind$ [] `(pigpen.runtime/keyword-field-selector->bind ~(mapv keyword fields))
                 {:field-type-out :native})
      (raw/generate$ (map-indexed raw/projection-field$ fields) {:field-type :native})
      (raw/store$ location :parquet {:schema schema}))))

(defmethod pigpen.pig-rx/store :parquet
  [{:keys [location fields opts]}]
  (let [schema (schema->pig-schema (:schema opts))]
    (StoreFuncStorage. (ParquetStorer.) schema location fields)))

(defmethod pigpen.script/storage->script [:store :parquet]
  [_]
  "parquet.pig.ParquetStorer()")
