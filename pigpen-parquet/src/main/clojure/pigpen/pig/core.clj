(ns pigpen.pig.core
  (:require [pigpen.local :as local]
            [pigpen.hadoop.core :as hadoop])
  (:import [org.apache.hadoop.mapreduce Job RecordWriter]
           [org.apache.pig LoadFunc StoreFunc ResourceSchema]
           [org.apache.pig.data Tuple]
           [org.apache.pig.impl.util Utils]))

(set! *warn-on-reflection* true)

(defn load-func->values
  [^LoadFunc load-func ^String location fields]
  (let [config (hadoop/config {})
        job (Job. config)
        _ (.setLocation load-func location job)
        _ (.setUDFContextSignature load-func (str (gensym)))
        
        job-context (hadoop/job-context job)
        input-format (.getInputFormat load-func)
        splits (.getSplits input-format job-context)]
    (mapcat identity
      (for [split splits]
        (let [task-context (hadoop/task-context job)
              record-reader (.createRecordReader input-format split task-context)]
          (.prepareToRead load-func record-reader nil)
          (.initialize record-reader split task-context)
          (->>
            (repeatedly #(.getNext load-func))
            (take-while identity)
            (map (fn [^Tuple v] (zipmap fields (.getAll v))))))))))

(defn load-load-func
  [load-func location fields]
  (local/load* location
               (constantly nil)
               (fn [_] (load-func->values load-func location fields))
               (constantly nil)))

(defn store-func->writer
  [^StoreFunc store-func ^String schema ^String location]
  (hadoop/clean location)
  
  (let [config (hadoop/config {})
        job (Job. config)
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

(defn store-store-func
  [^StoreFunc store-func ^String schema ^String location fields data]
  (local/store* data location
                #(store-func->writer store-func schema location)
                (fn [{:keys [^RecordWriter record-writer]} value]
                  (let [value' (->> fields
                                 (map value)
                                 (apply pigpen.pig/tuple))]
                    (.write record-writer nil value')))
                hadoop/close-record-writer))
