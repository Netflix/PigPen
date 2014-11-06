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

(ns pigpen.rx.functional-test
  (:require [clojure.test :refer [run-tests]]
            [pigpen.functional-test :as t :refer [TestHarness]]
            [pigpen.functional-suite :refer [def-functional-tests]]
            [pigpen.core :as pig]
            [pigpen.rx.core :as rx]))

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
        file)))
  #{
    pigpen.functional.join-test/test-fold-inline-sum
    pigpen.functional.join-test/test-fold-inline-count
    pigpen.functional.join-test/test-fold-count
    pigpen.functional.join-test/test-fold-cogroup-dual
    pigpen.functional.join-test/test-fold-all-sum
    pigpen.functional.join-test/test-fold-all-count
    pigpen.functional.join-test/test-fold-all-empty
    pigpen.functional.join-test/test-cogroup-self-join+fold
    pigpen.functional.join-test/test-join-inner-implicit
    pigpen.functional.join-test/test-join-inner
    pigpen.functional.join-test/test-join-left-outer
    pigpen.functional.join-test/test-join-right-outer
    pigpen.functional.join-test/test-join-full-outer
    pigpen.functional.join-test/test-join-inner-join-nils
    pigpen.functional.join-test/test-join-left-outer-join-nils
    pigpen.functional.join-test/test-join-right-outer-join-nils
    pigpen.functional.join-test/test-join-full-outer-join-nils
    pigpen.functional.join-test/test-join-self-join
    pigpen.functional.join-test/test-join-default-key-selector
    pigpen.functional.join-test/test-filter-by
    pigpen.functional.join-test/test-filter-by-nil-keys
    pigpen.functional.join-test/test-filter-by-duplicate-keys
    pigpen.functional.join-test/test-remove-by
    pigpen.functional.join-test/test-remove-by-nil-keys
    pigpen.functional.join-test/test-remove-by-duplicate-keys
    pigpen.functional.map-test/test-map+fold1
    pigpen.functional.map-test/test-map+fold2
    })
