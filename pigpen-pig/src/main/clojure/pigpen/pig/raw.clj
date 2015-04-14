;;
;;
;;  Copyright 2015 Netflix, Inc.
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

(ns pigpen.pig.raw
  (:require [pigpen.raw :refer [pigsym]]))

(defn register$
  "A Pig REGISTER command. jar is the qualified location of the jar."
  [jar]
  {:pre [(string? jar)]}
  ^:pig {:type :register
         :jar jar})

(defn option$
  "A Pig option. Takes the name and a value. Not used locally."
  [option value]
  {:pre [((some-fn string? keyword? symbol?) option)]}
  ^:pig {:type :option
         :option option
         :value value})

(defn return-debug$
  [data]
  {:pre [(sequential? data)
         (every? map? data)]}
  ^:pig {:type :return-debug
         :id (pigsym "return-debug")
         :fields (vec (keys (first data)))
         :data data})
