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

(ns pigpen.viz
  "Functions to create graph-viz graphs from PigPen expression graphs

Nothing in here will be used directly with normal PigPen usage.
See pigpen.core and pigpen.pig
"
  (:require [rhizome.viz :as viz]
            [pigpen.oven :as oven]))

(set! *warn-on-reflection* true)

(defn ^:private command->description [{:keys [id]}]
  "Returns a simple human readable description of a command"
  (str id))

(defn ^:private command->description+ [{:keys [id description]}]
  "Returns a verbose human readable description of a command"
  (if description
   (str id "\n\n" description)
   (str id)))

(def ^:private line-len 50)
(def ^:private max-lines 10)

(defn ^:private fix-label [label]
  (let [lines (clojure.string/split-lines label)
        label' (->> lines
                 (map (fn [l] (if (-> l count (> line-len)) (str (subs l 0 line-len) "...") l)))
                 (take max-lines)
                 (clojure.string/join "\\l"))]
    (if (> (count lines) max-lines)
      (str label' "\\l...")
      label')))

(defn ^:private view-graph [command->description commands]
  (viz/view-graph (filter #(contains? % :id) commands)
                  (fn [parent] (filter (fn [child] ((-> child :ancestors set) (:id parent))) commands))
                  :node->descriptor (fn [c] {:label (fix-label (command->description c))
                                             :shape :box})))

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
    (oven/bake :viz {} {})
    (view-graph command->description)))

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
    (oven/bake :viz {} {})
    (view-graph command->description+)))
