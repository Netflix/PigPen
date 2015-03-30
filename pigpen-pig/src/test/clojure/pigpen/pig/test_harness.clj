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

(ns pigpen.pig.test-harness
  (:require [pigpen.functional-test :as t :refer [TestHarness]]
            [pigpen.core :as pig]
            [pigpen.io :as pig-io]
            [pigpen.extensions.io :as io]
            [clojure.java.io :as jio]
            [pigpen.pig :as ppig]
            [pigpen.runtime :as rt]
            [pigpen.pig.local :as pig-local])
  (:import [org.apache.pig.pigunit PigTest]
           [org.apache.pig.builtin BinStorage]))

(defn list-files [file]
  (->>
    (io/list-files file)
    (remove #(.endsWith % ".crc"))
    (remove #(.endsWith % "_SUCCESS"))))

(defn run-script [harness command]
  (let [script-file (t/file harness)]
    (->> command
      (ppig/write-script script-file
                         {:extract-references? false
                          :add-pigpen-jar? false}))
    (doto (PigTest. script-file)
      (.unoverride "STORE")
      (.runScript))))

(defn run-script->output [harness command]
  (let [output-file (t/file harness)]
    (->> command
      (pig-io/store-binary output-file)
      (run-script harness))
    (->>
      (list-files output-file)
      (mapcat #(pig-local/load-func->values (BinStorage.) {} % [:value]))
      (map (comp rt/hybrid->clojure :value)))))

(defn pig-harness [prefix]
  (reify TestHarness
    (data [this data]
      (let [input-file (t/file this)]
        (spit input-file
              (->> data
                (map prn-str)
                (clojure.string/join)))
        (pig/load-clj input-file)))
    (dump [this command]
      (if (-> command :type #{:store :store-many})
        (run-script this command)
        (run-script->output this command)))
    (file [this]
      (str prefix (gensym)))
    (read [this file]
      (apply concat
        (for [f (list-files file)]
          (when-let [contents (not-empty (slurp f))]
            (clojure.string/split-lines contents)))))
    (write [this lines]
      (let [file (t/file this)]
        (spit file (clojure.string/join "\n" lines))
        file))))
