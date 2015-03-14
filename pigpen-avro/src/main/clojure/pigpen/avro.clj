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

(ns pigpen.avro
  "*** ALPHA - Subject to change ***

  Functions for reading avro data.

  See: http://avro.apache.org/

  Note: These are currently only supported by the local, rx, and pig platforms
"  (:require [pigpen.raw :as raw]
[pigpen.avro.core :as avro-core])
  (:import [org.apache.avro Schema]))

(set! *warn-on-reflection* true)

;; These namespaces need to be loaded to register multimethods. They are loaded
;; in a try/catch becasue not all of them are always available.
(def ^:private known-impls
  ['pigpen.local.avro
   'pigpen.pig.avro])

(doseq [ns known-impls]
  (try
    (require ns)
    (println (str "Loaded " ns))
    (catch Exception e
      #_(prn e))))

(defn load-avro
  "*** ALPHA - Subject to change ***

  Loads data from an avro file. Returns data as maps with keyword keys
corresponding to avro field names. Fields with avro type 'map' will be maps with
string keys.

  Example:

    (pig-avro/load-avro \"input.avro\" (slurp \"schemafile.json\"))

    (pig-avro/load-avro \"input.avro\"
       \"{\\\"namespace\\\": \\\"example.avro\\\",
          \\\"type\\\": \\\"record\\\",
          \\\"name\\\": \\\"foo\\\",
          \\\"fields\\\": [{\\\"name\\\": \\\"wurdz\\\",
                            \\\"type\\\": \\\"string\\\"},
                           {\\\"name\\\": \\\"bar\\\",
                            \\\"type\\\": \\\"int\\\"}]}\")

  Notes:
    * Avro schemas are defined on the project's website: http://avro.apache.org/docs/1.7.7/spec.html#schemas
    * load-avro takes the schema as a string
    * Make sure a piggybank.jar (http://mvnrepository.com/artifact/org.apache.pig/piggybank/0.14.0)
      compatible with your version of hadoop is on $PIG_CLASSPATH. For hadoop v2,
      see http://stackoverflow.com/a/21753749. Amazon's elastic mapreduce comes
      with a compatible piggybank.jar already on the classpath.
"
  {:added "0.2.13"}
  ([location schema]
    (let [^Schema parsed-schema (avro-core/parse-schema schema)
          fields (map symbol (avro-core/field-names parsed-schema))]
      (->>
        (raw/load$ location :avro fields {:schema parsed-schema})
        (raw/bind$
          '[pigpen.avro.core]
          '(pigpen.runtime/map->bind (comp
                                       pigpen.avro.core/dotted-keys->nested-map
                                       (pigpen.runtime/args->map pigpen.runtime/native->clojure)))
          {:args (clojure.core/mapcat (juxt str identity) fields)
           :field-type-in :native})))))
