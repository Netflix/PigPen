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

(ns pigpen.pig.parquet
  (:require [pigpen.pig.script])
  (:import [org.apache.parquet.pig PigSchemaConverter]))

(defmethod pigpen.pig.script/storage->script [:load :parquet]
  [command]
  (let [schema (->>
                 (get-in command [:opts :schema])
                 (.convert (PigSchemaConverter.))
                 str)
        ;; The schema converter wraps with {}
        schema (subs schema 1 (dec (count schema)))
        ;; In parquet, strings are stored as byte arrays
        schema (clojure.string/replace schema "bytearray" "chararray")
        ]
  (str "parquet.pig.ParquetLoader('" schema "')")))

(defmethod pigpen.pig.script/storage->script [:store :parquet]
  [_]
  "parquet.pig.ParquetStorer()")
