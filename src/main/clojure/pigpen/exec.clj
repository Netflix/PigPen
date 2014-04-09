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

(ns pigpen.exec
  "Contains functions related to script generation and manipulation. These are
how you 'use' a PigPen query.

  Note: Most of these are present in pigpen.core. Normally you should use those instead.
"
  (:require [pigpen.raw :as raw]
            [pigpen.pig :as pig]
            [pigpen.oven :as oven]
            [pigpen.script :as script]
            [pigpen.local :as local]
            [pigpen.viz :as viz]
            [taoensso.nippy :refer [freeze thaw]])
  (:import [rx Observable]
           [rx.observables BlockingObservable]))

(set! *warn-on-reflection* true)

(defn generate-script
  "Generates a Pig script from the relation specified and returns it as a string.
You can pass any relation to this and it will generate a Pig script - it doesn't
have to be an output. However, if there are no store commands, the script won't
do much. If you have more than one store command, use pigpen.core/script to
combine them. Optionally takes a map of options.

  Example:

    (pig/generate-script (pig/store-clj \"output.clj\" foo))
    (pig/generate-script {:debug \"/temp/\"} (pig/store-clj \"output.clj\" foo))

  Options:

    :debug - Enables debugging, which writes the output of every step to a file.
             The value is a path to place the debug output.

    :dedupe - Set to false to disable command deduping.

  See also: pigpen.core/write-script, pigpen.core/script
"
  {:added "0.1.0"}
  ([query] (generate-script {} query))
  ([opts query]
    (->> query
      (oven/bake opts)
      script/commands->script)))

(defn write-script
  "Generates a Pig script from the relation specified and writes it to location.
You can pass any relation to this and it will generate a Pig script - it doesn't
have to be an output. However, if there are no store commands, the script won't
do much. If you have more than one store command, use pigpen.core/script to
combine them. Optionally takes a map of options.

  Example:

    (pig/write-script \"my-script.pig\" (pig/store-clj \"output.clj\" foo))
    (pig/write-script \"my-script.pig\" {:debug \"/temp/\"} (pig/store-clj \"output.clj\" foo))

  Options:

    :debug - Enables debugging, which writes the output of every step to a file.
             The value is a path to place the debug output.

    :dedupe - Set to false to disable command deduping.

  See also: pigpen.core/generate-script, pigpen.core/script
"
  {:added "0.1.0"}
  ([location query] (write-script location {} query))
  ([location opts query]
    (spit location (generate-script opts query))))

(defn query->observable
  (^Observable [query] (query->observable {} query))
  (^Observable [opts query]
    (->> query
      (oven/bake opts)
      (local/graph->observable))))

(defn debug-script-raw [query]
  (->> query
    query->observable
    local/observable->raw-data))

(defn debug-script [query]
  (map 'value (debug-script-raw query)))

;; TODO add a version that returns a multiset
(defn dump
  "Executes a script locally and returns the resulting values as a clojure
sequence. This command is very useful for unit tests.

  Example:

    (->>
      (pig/load-clj \"input.clj\")
      (pig/map inc)
      (pig/filter even?)
      (pig/dump)
      (clojure.core/map #(* % %))
      (clojure.core/filter even?))

    (deftest test-script
      (is (= (->>
               (pig/load-clj \"input.clj\")
               (pig/map inc)
               (pig/filter even?)
               (pig/dump))
             [2 4 6])))

  Note: pig/store commands return an empty set
        pig/script commands merge their results

  See also: pigpen.core/show, pigpen.core/dump&show
"
  {:added "0.1.0"}
  ([query] (dump {} query))
  ([opts query]
    (->> query
      (query->observable opts)
      local/observable->data)))

(defn show
  "Generates a graph image for a PigPen query. This allows you to see what steps
will be executed when the script is run. The image is opened in another window.
This command uses a terse description for each operation.

  Example:

    (pigpen.core/show foo)

  See also: pigpen.core/show+, pigpen.core/dump&show
"
  {:added "0.1.0"}
  [query]
  (->> query
    (oven/bake {})
    (viz/view-graph viz/command->description)))

(defn show+
  "Generates a graph image for a PigPen query. This allows you to see what steps
will be executed when the script is run. The image is opened in another window.
This command uses a verbose description for each operation, including user code.

  Example:

    (pigpen.core/show+ foo)

  See also: pigpen.core/show, pigpen.core/dump&show+
"
  {:added "0.1.0"}
  [query]
  (->> query
    (oven/bake {})
    (viz/view-graph viz/command->description+)))
