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

(ns pigpen.cascading.oven
  (:require [pigpen.oven]))

(defn merge-reduce-fold
  "Merges a reduce command followed by a fold operation"
  [commands _]
  (let [lookup (->> commands
                 (map (juxt :id identity))
                 (into {}))]
    (->> commands
      (map (fn [c]
             (let [a (some-> c :ancestors first lookup)]
               (if (and (-> c :type #{:generate})
                        (-> a :type #{:reduce})
                        (->> c
                          :projections
                          (some (comp #{:fold} :udf :expr))))
                 {:type :reduce-fold
                  :id (:id c)
                  :ancestors (:ancestors a)
                  :reduce a
                  :fold c}
                 c)))))))

(defn merge-group-fold
  "Merges a group command followed by a fold operation"
  [commands _]
  (let [lookup (->> commands
                 (map (juxt :id identity))
                 (into {}))]
    (->> commands
      (map (fn [c]
             (let [a (some-> c :ancestors first lookup)]
               (if (and (-> c :type #{:generate})
                        (-> a :type #{:group})
                        (->> c
                          :projections
                          (some (comp #{:fold} :udf :expr))))
                 {:type :group-fold
                  :id (:id c)
                  :ancestors (:ancestors a)
                  :group a
                  :fold c}
                 c)))))))

;; **********

(def default-opts
  {})

(defn bake
  {:added "0.3.0"}
  ([query] (bake query {}))
  ([query opts]
    (pigpen.oven/bake
      query
      :cascading
      {merge-reduce-fold 4.1
       merge-group-fold  4.2}
      (merge default-opts opts))))
