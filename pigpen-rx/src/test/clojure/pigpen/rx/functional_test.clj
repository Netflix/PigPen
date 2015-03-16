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

(ns pigpen.rx.functional-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [pigpen.functional-test :as t :refer [TestHarness]]
            [pigpen.functional-suite :refer [def-functional-tests]]
            [pigpen.core :as pig]
            [pigpen.rx :as rx]))

(use-fixtures :once schema.test/validate-schemas)

(def prefix "build/functional/rx/")

(.mkdirs (java.io.File. prefix))

(def-functional-tests "rx"
  (reify TestHarness
    (data [this data]
      (pig/return data))
    (dump [this command]
      (rx/dump command))
    (file [this]
      (str prefix (gensym)))
    (read [this file]
      (clojure.string/split-lines
        (slurp file)))
    (write [this lines]
      (let [file (t/file this)]
        (spit file (clojure.string/join "\n" lines))
        file))))
