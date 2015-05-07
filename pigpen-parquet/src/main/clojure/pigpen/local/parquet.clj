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

(ns pigpen.local.parquet
  (:require [pigpen.local]
            [pigpen.hadoop]
            [pigpen.parquet.core :as pq])
  (:import [parquet.tools.read SimpleRecord SimpleRecord$NameValue]
           [pigpen.hadoop InputFormatLoader OutputFormatStorage]
           [parquet.hadoop ParquetInputFormat ParquetOutputFormat]))

(defmethod pigpen.local/load :parquet
  [{:keys [location fields]}]
  (let [field-names (into {} (map (juxt name identity) fields))]
    (InputFormatLoader.
      (ParquetInputFormat.)
      {ParquetInputFormat/READ_SUPPORT_CLASS "parquet.tools.read.SimpleReadSupport"}
      location
      (fn [^SimpleRecord value]
        (->> value
          (.getValues)
          (map (fn [^SimpleRecord$NameValue nv]
                 [(field-names (.getName nv)) (.getValue nv)]))
          (into {}))))))

(defmethod pigpen.local/store :parquet
  [{:keys [location opts]}]
  (OutputFormatStorage.
    (ParquetOutputFormat.)
    {ParquetOutputFormat/WRITE_SUPPORT_CLASS "pigpen.parquet.PigPenParquetWriteSupport"
     "schema" (str (:schema opts))}
    location))
