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

;; AWS EMR usage:
;; =============================================================================
;; run on master node of cluster:
;; -----------------------------------------------------------------------------
;; curl https://json-simple.googlecode.com/files/json-simple-1.1.1.jar > $PIG_CLASSPATH/json-simple-1.1.1.jar
;; cp /home/hadoop/.versions/2.4.0/share/hadoop/common/lib/avro-1.7.4.jar $PIG_CLASSPATH/
;; cp /home/hadoop/.versions/2.4.0/share/hadoop/common/lib/snappy-java-1.0.4.1.jar $PIG_CLASSPATH/

;; run locally in load-avro directory
;; -----------------------------------------------------------------------------
;; scp -i ~/schwartz-ci.pem ./target/load-avro-0.1.0-SNAPSHOT-standalone.jar hadoop@ec2-54-187-84-180.us-west-2.compute.amazonaws.com:$(ssh -i ~/schwartz-ci.pem hadoop@ec2-54-187-84-180.us-west-2.compute.amazonaws.com 'echo $PIG_CLASSPATH')/pigpen.jar
(defn load-avro
  ([location] (load-avro location {}))
  ([location opts] ;; you can add any other params here, like the args or field list
     (let [parsed-schema (parse-schema (:schema opts))
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
        (raw/load$             ;; this is a raw pig load command
         location              ;; the location of the data - this should be a string
         field-symbols
         storage               ;; this is the storage we created earlier
         opts)                 ;; just pass the opts through
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

(defmethod pigpen.local/load "org.apache.pig.piggybank.storage.avro.AvroStorage" [{:keys [location fields] }]
  (reify PigPenLocalLoader
    (locations [_] (pigpen.local/load-list location))
    (init-reader [_ filename]
      (-> filename (java.io.File.) (DataFileReader. (SpecificDatumReader.))))
    (read [_ reader] (for [datum reader] (zipmap fields (map #(attrs-lookup datum (str/split (name %) #"\.")) fields))))
    (close-reader [_ reader] (.close reader))))
