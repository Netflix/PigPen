;;
;;
;;  Copyright 2013-2015 Netflix, Inc.
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

(ns pigpen.pig.local
  (:require [pigpen.local :as local :refer [PigPenLocalLoader PigPenLocalStorage]]
            [pigpen.extensions.io :as io]
            [pigpen.hadoop :as hadoop]
            [pigpen.pig.runtime])
  (:import [org.apache.hadoop.mapreduce RecordWriter]
           [org.apache.pig LoadFunc StoreFuncInterface ResourceSchema]
           [org.apache.pig.data Tuple]
           [org.apache.pig.impl.util Utils]))

(set! *warn-on-reflection* true)

(defn load-func->values
  "Uses a Pig LoadFunc to read values from a file."
  [^LoadFunc load-func config-values ^String location fields]
  (let [config (hadoop/config config-values)
        job (hadoop/job config)
        _ (.setLocation load-func location job)
        _ (.setUDFContextSignature load-func (str (gensym)))

        job-context (hadoop/job-context job)
        input-format (.getInputFormat load-func)
        splits (.getSplits input-format job-context)]
    (apply concat
      (for [split splits]
        (let [task-context (hadoop/task-context job)
              record-reader (.createRecordReader input-format split task-context)]
          (.prepareToRead load-func record-reader nil)
          (.initialize record-reader split task-context)
          (->>
            (repeatedly #(.getNext load-func))
            (take-while identity)
            (map (fn [^Tuple v] (zipmap fields (.getAll v))))))))))

(deftype LoadFuncLoader [load-func config-values location fields]
  PigPenLocalLoader
  (locations [_]
    (local/load-list location))
  (init-reader [_ file] file)
  (read [_ file]
    (load-func->values load-func config-values file fields))
  (close-reader [_ _] nil))

(defn store-func->writer
  [^StoreFuncInterface store-func ^String schema ^String location]
  (io/clean location)

  (let [config (hadoop/config {})
        job (hadoop/job config)
        _ (.setStoreLocation store-func location job)
        _ (.setStoreFuncUDFContextSignature store-func (str (gensym)))

        schema' (Utils/getSchemaFromString schema)
        _ (.checkSchema store-func (ResourceSchema. schema'))

        task-context (hadoop/task-context job)
        output-format (.getOutputFormat store-func)
        record-writer (-> output-format
                        (.getRecordWriter task-context))]
    {:task-context task-context
     :output-format output-format
     :record-writer record-writer}))

(deftype StoreFuncStorage [store-func schema location fields]
  PigPenLocalStorage
  (init-writer [_]
    (store-func->writer store-func schema location))
  (write [_ {:keys [^RecordWriter record-writer]} value]
    (let [value' (->> fields
                   (map value)
                   (apply pigpen.pig.runtime/tuple))]
      (.write record-writer nil value')))
  (close-writer [_ writer]
    (hadoop/close-record-writer writer)))
