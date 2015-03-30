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

(ns pigpen.parquet.core
  (:import [parquet.io.api RecordConsumer Binary]
           [parquet.schema
            MessageType Type
            PrimitiveType
            PrimitiveType$PrimitiveTypeName]))

(set! *warn-on-reflection* true)

(defn schema->field-names [^MessageType schema]
  (for [^Type field (.getFields schema)]
    (symbol (.getName field))))

(defn schema->field-types [^MessageType schema]
  (for [^Type field (.getFields schema)]
    (do
      (when-not (.isPrimitive field)
        (throw (ex-info "non-primitive types are not supported yet" {:field field})))
      (condp = (.getPrimitiveTypeName ^PrimitiveType field)
        PrimitiveType$PrimitiveTypeName/BINARY  :string
        PrimitiveType$PrimitiveTypeName/BOOLEAN :boolean
        PrimitiveType$PrimitiveTypeName/INT32   :int
        PrimitiveType$PrimitiveTypeName/INT64   :long
        PrimitiveType$PrimitiveTypeName/DOUBLE  :double
        PrimitiveType$PrimitiveTypeName/FLOAT   :float))))

(defn write
  "Used with PigPenParquetWriteSupport"
  [^RecordConsumer consumer
   ^MessageType schema
   record]
  (.startMessage consumer)
  (let [values (->> record
                 (map (fn [[k v]] [(name k) v]))
                 (into {}))
        fields (.getFields schema)]
    (doseq [[i ^Type field] (map-indexed vector fields)]
      (let [field-name (-> field .getName)
            value (get values field-name)]
        (if (.isPrimitive field)
          (do
            (.startField consumer field-name i)
            (condp = (.getPrimitiveTypeName ^PrimitiveType field)
              PrimitiveType$PrimitiveTypeName/BINARY  (.addBinary consumer (Binary/fromString value))
              PrimitiveType$PrimitiveTypeName/BOOLEAN (.addBoolean consumer value)
              PrimitiveType$PrimitiveTypeName/INT32   (.addInteger consumer value)
              PrimitiveType$PrimitiveTypeName/INT64   (.addLong consumer value)
              PrimitiveType$PrimitiveTypeName/DOUBLE  (.addDouble consumer value)
              PrimitiveType$PrimitiveTypeName/FLOAT   (.addFloat consumer value))
            (.endField consumer field-name i))

          ;else
          (throw (ex-info "Unsupported field type" {:field field
                                                    :index i}))))))
  (.endMessage consumer))
