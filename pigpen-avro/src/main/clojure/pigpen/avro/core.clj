;;
;;
;;  Copyright 2013 Netflix, Inc.
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

(ns pigpen.avro.core
  (:require [clojure.string :as str]
            [pigpen.raw :as raw]
            [pigpen.core :as pig])
  (:import [org.apache.avro
            Schema$Type
            Schema$Parser
            SchemaNormalization]
           [org.apache.avro.file DataFileReader]
           [org.apache.avro.specific SpecificDatumReader]
           [pigpen.local PigPenLocalLoader]))

(declare field-names)
(defn prefixed-names [record-schema prefix]
  (for [subfield (field-names record-schema)]
    (str prefix "." subfield)))

(defn field-names [parsed-schema]
 (->> (.getFields parsed-schema)
       (map (fn [field]
              (let [type (.getType (.schema field))]
                (cond
                 (= type Schema$Type/RECORD) (prefixed-names (.schema field) (.name field))
                 ;; pig only supports union of null, <type>.
                 (= type Schema$Type/UNION)  (let [record-fields (->> (.getTypes (.schema field))
                                                                      (filter #(= (.getType %) Schema$Type/RECORD))
                                                                      (map (fn [rec] (prefixed-names rec (.name field)))))]
                                               (if (empty? record-fields) [(.name field)]
                                                   record-fields))
                 :else                       (.name field)))))
       flatten
       vec))

(defn parse-schema [s] (.parse (Schema$Parser.) s))

(defmethod pigpen.pig/native->clojure org.apache.avro.util.Utf8 [value]
  (str value))

(defn dotted-keys->nested-map [kvs]
  (->> kvs
   (map (fn [[k v]] [(-> k name (clojure.string/split #"\.")) v]))
   (reduce (fn [acc [ks v]]
             (try
              (if v (assoc-in acc (map keyword ks) v) acc)
              (catch Exception e (throw (Exception. (str "can't assoc-in record: " acc "\nkeys: " ks "\nvalue: " v "\nkvs: " kvs)))))) {})))


(defn load-avro
  "*** ALPHA - Subject to change ***

  Loads data from an avro file. Returns data as maps with keyword keys corresponding to avro field names. Fields with avro type \"map\" will be maps with string keys.

  Example:

    ;; avro schemas are defined [on the project's website](http://avro.apache.org/docs/1.7.7/spec.html#schemas)
    (pig-avro/load-avro \"input.avro\" (slurp \"schemafile.json\"))

    (pig-avro/load-avro \"input.avro\"
       {\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"foo\",
                       \"fields\": [{\"name\": \"wurdz\", \"type\": \"string\"}, {\"name\": \"bar\", \"type\": \"int\"}] })

"
  ([location schema]
     (let [parsed-schema (parse-schema schema)
           storage (raw/storage$ ;; this creates a new storage definition
                    ;; add these jars to $PIG_CLASSPATH (most likely /home/hadoop/pig/lib)
                    ["json-simple-1.1.1.jar"
                     "piggybank.jar"
                     "avro-1.7.4.jar"
                     "snappy-java-1.0.4.1.jar"
                     ]
                    "org.apache.pig.piggybank.storage.avro.AvroStorage" ;; your loader class
                    ["schema" (SchemaNormalization/toParsingForm parsed-schema)])
           field-symbols (map symbol (field-names parsed-schema))]
       (->
        (raw/load$ location field-symbols storage {:implicit-schema true})
        (raw/bind$
         '[pigpen.avro.core]
         '(pigpen.pig/map->bind (comp
                                 pigpen.avro.core/dotted-keys->nested-map
                                 (pigpen.pig/args->map pigpen.pig/native->clojure)))
         {:args (clojure.core/mapcat (juxt str identity) field-symbols)   ;; tell it what input fields to use
          :field-type-in :native })))))

(defn attrs-lookup [record attr-names]
  (cond
   (nil? record) nil
   (empty? attr-names) record
   :else (attrs-lookup (.get record (first attr-names)) (rest attr-names))))

(defmethod pigpen.local/load "org.apache.pig.piggybank.storage.avro.AvroStorage"
  [{:keys [location fields] }]
  (reify PigPenLocalLoader
    (locations [_] (pigpen.local/load-list location))
    (init-reader [_ filename]
      (-> filename (java.io.File.) (DataFileReader. (SpecificDatumReader.))))
    (read [_ reader]
      (for [datum reader]
        (zipmap fields (map #(attrs-lookup datum (str/split (name %) #"\.")) fields))))
    (close-reader [_ reader] (.close reader))))
