;;
;;
;;  Copyright 2013-2015 Netflix, Inc.
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

(ns pigpen.rx
  "A high performance dump operator. The default implementation in pigpen.core
uses lazy seqs, which can be inefficient on larger data. The rx implementation
uses rx-java to deliver a slightly more performance for large local datasets.
"
  (:require [pigpen.rx.core :as rx]
            [pigpen.rx.extensions :refer [multicast->observable]]
            [rx.lang.clojure.blocking :as rx-blocking]
            [pigpen.local :as local]
            [pigpen.oven :as oven]))

(defn dump
  "Executes a script locally and returns the resulting values as a clojure
sequence. This command is very useful for unit tests.

  Example:

    (->>
      (pig/load-clj \"input.clj\")
      (pig/map inc)
      (pig/filter even?)
      (pig-rx/dump)
      (clojure.core/map #(* % %))
      (clojure.core/filter even?))

    (deftest test-script
      (is (= (->>
               (pig/load-clj \"input.clj\")
               (pig/map inc)
               (pig/filter even?)
               (pig-rx/dump))
             [2 4 6])))

  Note: pig/store commands return the output data
        pig/store-many commands merge their results
"
  {:added "0.1.0"}
  [query]
  (let [state {:code-cache (atom {})}
        graph (oven/bake :rx {} {} query)
        last-command (:id (last graph))]
    (->> graph
      (reduce (partial rx/graph->observable+ state) {})
      (last-command)
      (multicast->observable)
      (rx-blocking/into [])
      (map (comp local/remove-sentinel-nil val first)))))
