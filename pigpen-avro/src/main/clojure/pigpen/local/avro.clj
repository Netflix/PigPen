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

(ns pigpen.local.avro
  (:require [pigpen.local]
            [pigpen.avro.core :as avro-core])
  (:import [org.apache.avro Schema]
           [org.apache.avro.file DataFileReader]
           [org.apache.avro.specific SpecificDatumReader]
           [org.apache.avro.generic GenericData$Record GenericData$Array GenericData$EnumSymbol]
           [pigpen.local PigPenLocalLoader]))

(set! *warn-on-reflection* true)

(defn attrs-lookup [^GenericData$Record record attr-names]
  (cond
    (nil? record) nil
    (empty? attr-names) record
    :else (attrs-lookup (.get record ^{:tag java.lang.String} (first attr-names)) (rest attr-names))))

(defmethod pigpen.local/load :avro
  [{:keys [location fields opts]}]
  (reify PigPenLocalLoader
    (locations [_] (pigpen.local/load-list location))
    (init-reader [_ filename]
      (-> ^java.lang.String filename
        (java.io.File.)
        (DataFileReader.
          (SpecificDatumReader. ^Schema (:schema opts)))))
    (read [_ reader]
      (for [datum ^SpecificDatumReader reader]
        (zipmap fields (map #(attrs-lookup datum (clojure.string/split (name %) #"\.")) fields))))
    (close-reader [_ reader] (.close ^{:tag java.io.Closeable} reader))))
