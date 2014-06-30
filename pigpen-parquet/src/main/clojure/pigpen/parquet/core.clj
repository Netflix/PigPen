(ns pigpen.parquet.core
  (:require [pigpen.raw :as raw]
            [pigpen.local]
            [pigpen.hadoop.core :as hadoop]
            [pigpen.pig.core :as pig-storage])
  (:import [parquet.pig ParquetLoader ParquetStorer]))

(set! *warn-on-reflection* true)

(defn schema->pig-schema [schema]
  (->> schema
    (map (fn [[field type]] (str (name field) ":" (name type))))
    (clojure.string/join ",")))

(defn load-parquet
  "Schema can be either a map, where it is used literally, or a sequence, where
it is used to select fields. Either strings, keywords, or symbols can be used
for fields."
  [location schema]
  (let [fields (->> schema keys (mapv (comp symbol name)))
        pig-schema (schema->pig-schema schema)
        storage (raw/storage$ [] "parquet.pig.ParquetLoader" [pig-schema])]
    (-> location
      (raw/load$ fields storage {:implicit-schema true})
      (raw/bind$ [] '(pigpen.pig/map->bind (pigpen.pig/args->map pigpen.pig/native->clojure))
                 {:args (clojure.core/mapcat (juxt str identity) fields), :field-type-in :native}))))

(defmethod pigpen.local/load "parquet.pig.ParquetLoader"
  [{:keys [location fields storage]}]
  (let [schema (first (:args storage))]
    (pig-storage/load-load-func (ParquetLoader. schema) location fields)))

(defn store-parquet
  [location schema relation]
  (let [fields (map (comp symbol name) (keys schema))
        storage (raw/storage$ [] "parquet.pig.ParquetStorer" [])]
    (-> relation
      (raw/bind$ [] `(pigpen.pig/keyword-field-selector->bind ~(mapv keyword fields))
                 {:field-type-out :native
                  :implicit-schema true})
      (raw/generate$ (map-indexed raw/projection-field$ fields) {:field-type :native})
      (raw/store$ location storage {:schema schema}))))

(defmethod pigpen.local/store "parquet.pig.ParquetStorer"
  [{:keys [location fields opts]} data]
  (let [schema (schema->pig-schema (:schema opts))]
    (pig-storage/store-store-func (ParquetStorer.) schema location fields data)))
