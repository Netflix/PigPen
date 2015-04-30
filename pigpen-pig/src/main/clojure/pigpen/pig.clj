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

(ns pigpen.pig
  "Functions to convert a PigPen query into a Pig script.
"
  (:require [pigpen.raw :as raw]
            [pigpen.pig.oven :as oven]
            [pigpen.pig.script :as script]))

(set! *warn-on-reflection* true)

(defn generate-script
  "Generates a Pig script from the relation specified and returns it as a string.
You can pass any relation to this and it will generate a Pig script - it doesn't
have to be an output. However, if there are no store commands, the script won't
do much. If you have more than one store command, use pigpen.core/store-many to
combine them. Optionally takes a map of options.

  Example:

    (generate-script (pig/store-clj \"output.clj\" foo))
    (generate-script {:debug \"/temp/\"} (pig/store-clj \"output.clj\" foo))

  Options:

    :debug - Enables debugging, which writes the output of every step to a file.
             The value is a path to place the debug output.

    :dedupe - Set to false to disable command deduping.

    :pigpen-jar-location - The location where your uberjar resides.
                           Defaults to 'pigpen.jar'.

  See also: pigpen.pig/write-script, pigpen.core/store-many
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
do much. If you have more than one store command, use pigpen.core/store-many to
combine them. Optionally takes a map of options.

  Example:

    (write-script \"my-script.pig\" (pig/store-clj \"output.clj\" foo))
    (write-script \"my-script.pig\" {:debug \"/temp/\"} (pig/store-clj \"output.clj\" foo))

  Options:

    :debug - Enables debugging, which writes the output of every step to a file.
             The value is a path to place the debug output.

    :dedupe - Set to false to disable command deduping.

    :pigpen-jar-location - The location where your uberjar resides.
                           Defaults to 'pigpen.jar'.

  See also: pigpen.pig/generate-script, pigpen.core/store-many
"
  {:added "0.1.0"}
  ([location query] (write-script location {} query))
  ([location opts query]
    (spit location (generate-script opts query))))

(defn set-options
  "Used to set options in a pig script. `opts` is a map of string/keyword/symbol
to anything. str is called on values.

  Example:

    (set-options {:pig.maxCombinedSplitSize 1000000} relation)

  Note: Pig options are global and apply to the entire script.
"
  [opts relation]
  (update-in relation [:opts :pig-options] merge opts))
