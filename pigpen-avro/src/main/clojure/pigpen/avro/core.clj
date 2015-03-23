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

(ns pigpen.avro.core
  (:require [pigpen.runtime :as rt :refer [NativeToClojure]]
            [pigpen.raw :as raw])
  (:import [org.apache.avro
            Schema
            Schema$Field
            Schema$Type
            Schema$Parser]
           [org.apache.avro.generic
            GenericData$Record
            GenericData$Array
            GenericData$EnumSymbol]
           [org.apache.avro.util Utf8]))

(set! *warn-on-reflection* true)

(extend-protocol NativeToClojure
  Utf8
  (rt/native->clojure [value]
    (str value))
  GenericData$Array
  (rt/native->clojure [value]
    (mapv rt/native->clojure value))
  GenericData$Record
  (rt/native->clojure [^GenericData$Record value]
    (let [fields (->> value .getSchema .getFields (map (fn [^Schema$Field fd] (.name fd))))]
      (zipmap (map keyword fields)
              (map #(rt/native->clojure
                      (.get value ^String %)) fields))))
  GenericData$EnumSymbol
  (rt/native->clojure [value]
    (str value)))

(declare field-names)

(defn prefixed-names [record-schema prefix]
  (for [subfield (field-names record-schema)]
    (str prefix "." subfield)))

(defn field-names [^Schema parsed-schema]
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

(defn parse-schema [^String s]
  (.parse ^{:tag Schema$Parser} (Schema$Parser.) s))

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
