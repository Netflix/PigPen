(ns pigpen.hadoop.core
  (:require [pigpen.local :as local])
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

(defn clean [out-dir]
  (if (string? out-dir)
    (clean (File. ^String out-dir))
    (when (.exists ^File out-dir)
      (when (.isDirectory ^File out-dir)
        (doseq [file (.listFiles ^File out-dir)]
          (clean file)))
      (.delete ^File out-dir))))

(defn config [initial-values]
  (let [config (Configuration.)]
    (doseq [[k v] initial-values]
      (.set config k v))
    config))

(defn job-context [^Job job]
  (JobContext.
    (.getConfiguration job)
    (JobID. "jt" 0)))

(defn task-context [^Job job]
  (TaskAttemptContext.
    (.getConfiguration job)
    (TaskAttemptID. "jt" 0 true 1 0)))

(defn input-format->values
 [^InputFormat input-format config-values ^String location]
 (let [config (config config-values)
       job (Job. config)
       _ (FileInputFormat/setInputPaths job location)
        
       job-context (job-context job)
       splits (.getSplits input-format job-context)]
   (mapcat identity
     (for [split splits]
       (let [task-context (task-context job)
             record-reader (-> input-format
                             (.createRecordReader split task-context))]
         (.initialize record-reader split task-context)
         (->>
           (repeatedly #(when (.nextKeyValue record-reader)
                          (.getCurrentValue record-reader)))
           (take-while identity)))))))

(defn load-input-format
  [input-format config-values location]
  (local/load* location
               (constantly nil)
               (fn [_] (input-format->values input-format config-values location))
               (constantly nil)))

(defn output-format->writer
  [^OutputFormat output-format ^String location]
  (clean location)
  
  (let [config (config {})
        job (Job. config)
        _ (FileOutputFormat/setOutputPath job location)
        
        task-context (task-context job)
        record-writer (-> output-format
                        (.getRecordWriter task-context))]
    {:task-context task-context
     :output-format output-format
     :record-writer record-writer}))

(defn close-record-writer
  [{:keys [^TaskAttemptContext task-context
           ^OutputFormat output-format
           ^RecordWriter record-writer]}]
  (-> record-writer
      (.close task-context))
   
  (-> output-format
    (.getOutputCommitter task-context)
    (.commitTask  task-context)))

(defn store-output-format
  [output-format location data]
  (local/store* data location
                #(output-format->writer output-format location)
                (fn [{:keys [^RecordWriter record-writer]} value]
                           (.write record-writer nil value))
                close-record-writer))
