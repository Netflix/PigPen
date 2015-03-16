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

(ns pigpen.cascading
    "Functions to convert a PigPen query into a Cascading flow.
"
  (:require [pigpen.cascading.core :as cascading]
            [pigpen.raw :as raw]
            [pigpen.cascading.oven :as oven])
  (:import [cascading.tap Tap]
           [cascading.flow.hadoop HadoopFlowConnector]))

;; ********** Flow **********

(defn generate-flow
  "Transforms the relation specified into a Cascading flow that is ready to be
executed.

Optionally takes a Cascading FlowConnector (defaults to HadoopFlowConnector)

  Example:

    (generate-flow (pig/store-clj \"output.clj\" foo))
"
  {:added "0.3.0"}
  ([query] (generate-flow (HadoopFlowConnector.) query))
  ([connector query]
    (->> query
      (oven/bake {})
      (cascading/commands->flow connector))))

;; ********** Customer loaders **********

;; TODO this needs to be a macro
(defn load-tap
  "A thin wrapper around a tap. By default a vector of the tap's source fields
is created and returned as a single field. A custom function can be provided to
map the tap's source fields onto a single value.

  Example:

    (load-tap tap)
    (load-tap tap (partial zipmap [:a :b :c]))

"
  {:added "0.3.0"}
  ([^Tap tap]
    (load-tap tap 'clojure.core/vector))
  ([^Tap tap f]
    (let [fields (mapv symbol (.getSourceFields tap))]
      (->>
        (raw/load$ (.toString tap) :tap fields {:tap tap})
        (raw/bind$
          `(pigpen.runtime/map->bind ~f)
          {:field-type-in :native})))))

;; TODO take a list of field names to project from a map
(defn store-tap
  "A thin wrapper around a sink tap. The tap must accept a single sink field
which is the value to store."
  {:added "0.3.0"}
  [^Tap tap relation]
  {:pre [(<= (.size (.getSinkFields tap)) 1)]}
  (->> relation
    (raw/bind$ `(pigpen.runtime/map->bind identity)
               {:alias (or (seq (map symbol (.getSinkFields tap)))
                           (:fields relation))
                :field-type :native})
    (raw/store$ (.toString tap) :tap {:tap tap})))
