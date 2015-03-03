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
            Schema
            Schema$Field
            Schema$Type
            Schema$Parser
            SchemaNormalization]
           [org.apache.avro.file DataFileReader]
           [org.apache.avro.specific SpecificDatumReader]
           [org.apache.avro.generic GenericData$Record GenericData$Array GenericData$EnumSymbol]
           [pigpen.local PigPenLocalLoader]))

(set! *warn-on-reflection* true)

(declare field-names)
(defn- prefixed-names [record-schema prefix]
  (for [subfield (field-names record-schema)]
    (str prefix "." subfield)))

(defn- field-names [^Schema parsed-schema]
 (->> (.getFields parsed-schema)
       (map (fn [^Schema$Field field]
              (let [type (.getType ^{:tag Schema} (.schema field))]
                (cond
                 (= type Schema$Type/RECORD) (prefixed-names (.schema field) (.name field))
                 ;; pig only supports union of null, <type>.
                 (= type Schema$Type/UNION)  (let [record-fields (->> (.getTypes (.schema field))
                                                                      (filter #(= (.getType ^{:tag Schema} %) Schema$Type/RECORD))
                                                                      (map (fn [rec] (prefixed-names rec (.name field)))))]
                                               (if (empty? record-fields) [(.name field)]
                                                   record-fields))
                 :else                       (.name field)))))
       flatten
       vec))

(defn- parse-schema [^String s]
  (.parse ^{:tag Schema$Parser} (Schema$Parser.) s))

(defmethod pigpen.pig/native->clojure org.apache.avro.util.Utf8 [value]
  (str value))

(defmethod pigpen.pig/native->clojure GenericData$Array [value]
  (vec (map pigpen.pig/native->clojure value)))

(defmethod pigpen.pig/native->clojure GenericData$Record [^GenericData$Record value]
  (let [fields (->> value .getSchema .getFields (map (fn [^Schema$Field fd] (.name fd))))]
    (zipmap (map keyword fields) (map #(pigpen.pig/native->clojure
                                        (.get value ^{:tag String} %)) fields))))

(defmethod pigpen.pig/native->clojure GenericData$EnumSymbol [value]
  (str value))

(defn dotted-keys->nested-map [kvs]
  (->> kvs
   (map (fn [[k v]] [(-> k name (clojure.string/split #"\.")) v]))
   (reduce (fn [acc [ks v]]
             (try
              (if v (assoc-in acc (map keyword ks) v) acc)
              (catch Exception e
                (throw (Exception.
                        (str "can't assoc-in record: " acc "\nkeys: "
                             ks "\nvalue: " v "\nkvs: " kvs)))))) {})))

(defn load-avro
  "*** ALPHA - Subject to change ***

  Loads data from an avro file. Returns data as maps with keyword keys corresponding to avro field names. Fields with avro type \"map\" will be maps with string keys.

  Example:

    ;; avro schemas are defined [on the project's website](http://avro.apache.org/docs/1.7.7/spec.html#schemas)
    ;; load-avro takes the schema as a string.
    (pig-avro/load-avro \"input.avro\" (slurp \"schemafile.json\"))

    (pig-avro/load-avro \"input.avro\"
       \"{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"foo\",
  \"fields\": [{\"name\": \"wurdz\", \"type\": \"string\"}, {\"name\": \"bar\", \"type\": \"int\"}] }\")

  Make sure a
  [piggybank.jar](http://mvnrepository.com/artifact/org.apache.pig/piggybank/0.14.0)
  compatible with your version of hadoop is on $PIG_CLASSPATH. For
  hadoop v2, see http://stackoverflow.com/a/21753749. Amazon's elastic
  mapreduce comes with a compatible piggybank.jar already on the
  classpath.
"
  ([location schema]
     (let [^Schema parsed-schema (parse-schema schema)
           storage (raw/storage$
                    ["piggybank.jar"]
                    "org.apache.pig.piggybank.storage.avro.AvroStorage"
                    ["schema" (.toString parsed-schema)])
           field-symbols (map symbol (field-names parsed-schema))]
       (->
        (raw/load$ location field-symbols storage {:implicit-schema true})
        (raw/bind$
         '[pigpen.avro.core]
         '(pigpen.pig/map->bind (comp
                                 pigpen.avro.core/dotted-keys->nested-map
                                 (pigpen.pig/args->map pigpen.pig/native->clojure)))
         {:args (clojure.core/mapcat (juxt str identity) field-symbols)
          :field-type-in :native })))))

(defn- attrs-lookup [^GenericData$Record record attr-names]
  (cond
   (nil? record) nil
   (empty? attr-names) record
   :else (attrs-lookup (.get record ^{:tag java.lang.String} (first attr-names)) (rest attr-names))))

(defmethod pigpen.local/load "org.apache.pig.piggybank.storage.avro.AvroStorage"
  [{:keys [location fields storage]}]
  (reify PigPenLocalLoader
    (locations [_] (pigpen.local/load-list location))
    (init-reader [_ filename]
      (-> ^java.lang.String filename
          (java.io.File.)
          (DataFileReader.
           (SpecificDatumReader. ^Schema (parse-schema (-> storage :args second))))))
    (read [_ reader]
      (for [datum ^SpecificDatumReader reader]
        (zipmap fields (map #(attrs-lookup datum (str/split (name %) #"\.")) fields))))
    (close-reader [_ reader] (.close ^{:tag java.io.Closeable} reader))))
