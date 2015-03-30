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

(ns pigpen.hadoop
  "Methods to help use hadoop stuff locally"
  (:require [pigpen.local :as local :refer [PigPenLocalLoader PigPenLocalStorage]]
            [pigpen.extensions.io :as io])
  (:import [java.io File]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapreduce InputFormat InputSplit OutputFormat
            RecordReader RecordWriter
            Job JobID JobContext
            TaskAttemptID TaskAttemptContext]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]))

(set! *warn-on-reflection* true)

(defn config
  "Create a Hadoop Configuration"
  [initial-values]
  (let [config (Configuration.)]
    (doseq [[k v] initial-values]
      (.set config k v))
    config))

(defn job
  "Create a Hadoop Job"
  [^Configuration config]
  (Job. config))

(defn job-context
  "Create a Hadoop JobContext"
  [^Job job]
  (JobContext.
    (.getConfiguration job)
    (JobID. "jt" 0)))

(defn task-context
  "Create a Hadoop TaskAttemptContext"
  [^Job job]
  (TaskAttemptContext.
    (.getConfiguration job)
    (TaskAttemptID. "jt" 0 true 1 0)))

(defn input-format->values
  "Uses a Hadoop InputFormat to read values from a file."
  [^InputFormat input-format config-values ^String location f]
  (let [config (config config-values)
        ^Job job (job config)
        _ (FileInputFormat/setInputPaths job location)

        job-context (job-context job)
        splits (.getSplits input-format job-context)]
    (apply concat
      (for [split splits]
        (let [task-context (task-context job)
              record-reader (-> input-format
                              (.createRecordReader split task-context))]
          (.initialize record-reader split task-context)
          (->>
            (repeatedly #(when (.nextKeyValue record-reader)
                           (.getCurrentValue record-reader)))
            (take-while identity)
            (map f)))))))

(deftype InputFormatLoader [input-format config-values location f]
  PigPenLocalLoader
  (locations [_]
    (local/load-list location))
  (init-reader [_ file] file)
  (read [_ file]
    (input-format->values input-format config-values file f))
  (close-reader [_ _] nil))

(defn output-format->writer
  "Uses a Hadoop OutputFormat to write values to a file. This creates the writer."
  [^OutputFormat output-format config-values ^String location]
  (io/clean location)

  (let [config (config config-values)
        job (job config)
        _ (FileOutputFormat/setOutputPath job (Path. location))

        task-context (task-context job)
        record-writer (-> output-format
                        (.getRecordWriter task-context))]
    {:task-context task-context
     :output-format output-format
     :record-writer record-writer}))

(defn write-record-writer
  "Writes a value to an OutputFormat writer"
  [{:keys [^RecordWriter record-writer]} value]
  (.write record-writer nil value))

(defn close-record-writer
  "Closes and commits an OutputFormat"
  [{:keys [^TaskAttemptContext task-context
           ^OutputFormat output-format
           ^RecordWriter record-writer]}]
  (-> record-writer
    (.close task-context))
  (-> output-format
    (.getOutputCommitter task-context)
    (.commitTask  task-context)))

(deftype OutputFormatStorage [output-format config-values location]
  PigPenLocalStorage
  (init-writer [_]
    (output-format->writer output-format config-values location))
  (write [_ writer value]
    (write-record-writer writer value))
  (close-writer [_ writer]
    (close-record-writer writer)))
