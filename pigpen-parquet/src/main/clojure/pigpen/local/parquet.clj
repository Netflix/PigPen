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
            [pigpen.parquet.core :as pq]
            [pigpen.pig.local])
  (:import [parquet.pig ParquetLoader ParquetStorer]
           [pigpen.pig.local LoadFuncLoader StoreFuncStorage]))

(defmethod pigpen.local/load :parquet
  [{:keys [location fields storage]}]
  (let [schema (first (:args storage))]
    (LoadFuncLoader. (ParquetLoader. schema) {} location fields)))

(defmethod pigpen.local/store :parquet
  [{:keys [location args opts]}]
  (let [schema (pq/schema->pig-schema (:schema opts))]
    (StoreFuncStorage. (ParquetStorer.) schema location args)))
