;;
;;
;;  Copyright 2014-2015 Netflix, Inc.
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
  "Functions for reading and writing parquet data and for creating parquet
schemas. Start with `load-parquet` and `store-parquet`.

  See: http://parquet.incubator.apache.org/

  Note: These are currently only supported by the local, rx, and pig platforms.

  Note: There are inconsistencies with how strings & byte arrays are stored.
        Parquet stores strings as byte arrays, but loses the type of the
        original value. PigPen will read all byte arrays as strings.
"
  (:refer-clojure :exclude [float double boolean byte-array])
  (:require [pigpen.raw :as raw]
            [pigpen.parquet.core :as pq])
  (:import [parquet.schema
            MessageType GroupType
            PrimitiveType PrimitiveType$PrimitiveTypeName
            Type Type$Repetition]))

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

;; Schema

(def ^:private default-repetition :required)

(defn ^:private repetition->TypeRepetition [repetition]
  (case (or repetition default-repetition)
    :required Type$Repetition/REQUIRED
    :optional Type$Repetition/OPTIONAL
    :repeated Type$Repetition/REPEATED))

(defmacro ^:private defprimitive [name type]
  `(defn ~name
     ~(str "Defines a field of type " (clojure.core/name type) " in a parquet schema.
`repitition` is one of :required, :optional, or :repeated; defaults to :required.
")
     ([~'name] (~name ~'name ~'default-repetition))
     ([~'name ~'repetition]
       (PrimitiveType. (repetition->TypeRepetition ~'repetition) ~type ~'name))))

(defprimitive int32      PrimitiveType$PrimitiveTypeName/INT32)
(defprimitive int64      PrimitiveType$PrimitiveTypeName/INT64)
;(defprimitive int96      PrimitiveType$PrimitiveTypeName/INT96)
(defprimitive float      PrimitiveType$PrimitiveTypeName/FLOAT)
(defprimitive double     PrimitiveType$PrimitiveTypeName/DOUBLE)
(defprimitive boolean    PrimitiveType$PrimitiveTypeName/BOOLEAN)
(defprimitive binary     PrimitiveType$PrimitiveTypeName/BINARY)
;(defprimitive byte-array PrimitiveType$PrimitiveTypeName/FIXED_LEN_BYTE_ARRAY)

;(defn group
;  [name & fields]
;  (if-not (keyword? name)
;    (apply group default-repetition name fields)
;    (let [repetition name
;          [name & fields] fields]
;      (GroupType. (repetition->TypeRepetition repetition) name fields))))

(defn message
  "Defines a parquet schema. `name` is a string. To define fields, see:

  pigpen.parquet/int32
  pigpen.parquet/int64
  pigpen.parquet/float
  pigpen.parquet/double
  pigpen.parquet/boolean
  pigpen.parquet/binary (used for strings)

  Note: Complex data structures (GroupType) are not supported at this time.
"
  [name & fields]
  (MessageType. name fields))

;; Commands

(defn load-parquet
  "Loads data from a parquet file. Returns data as maps with keywords matching
the parquet column names. The parameter `schema` is a parquet schema.

  Example:

    (load-parquet \"input.pq\" (message (int64 \"value\")))

  See also: pigpen.parquet/message for schema details

  See also: https://github.com/apache/incubator-parquet-mr
"
  {:added "0.2.7"}
  [location schema]
  (let [fields (pq/schema->field-names schema)]
    (->>
      (raw/load$ location :parquet fields {:schema schema})
      (raw/bind$ '(pigpen.runtime/map->bind (pigpen.runtime/args->map pigpen.runtime/native->clojure))
                 {:args (clojure.core/mapcat (juxt name identity) fields)
                  :field-type-in :native}))))

(defn store-parquet
  "Stores data to a parquet file. The relation prior to this command must be a
map with keywords matching the parquet columns to be stored. The parameter
`schema` is a parquet schema.

  Example:

    (store-parquet \"output.pq\" (message (int64 \"value\")) foo)

  See also: pigpen.parquet/message for schema details

  See also: https://github.com/apache/incubator-parquet-mr
"
  {:added "0.2.7"}
  [location schema relation]
  (let [fields (pq/schema->field-names schema)]
    (->> relation
      (raw/bind$ `(pigpen.runtime/keyword-field-selector->bind ~(mapv keyword fields))
                 {:field-type :native
                  :alias fields
                  :types (pq/schema->field-types schema)})
      (raw/store$ location :parquet {:schema schema}))))
