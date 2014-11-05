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

(ns pigpen.cascading.functional-test
  (:require [clojure.test :refer [run-tests]]
            [pigpen.functional-test :as t :refer [TestHarness]]
            [pigpen.functional-suite :refer [def-functional-tests]]
            [pigpen.core :as pig]
            [pigpen.extensions.io :as io]
            [pigpen.cascading.core :as cascading]))

(def prefix "build/functional/cascading/")

(.mkdirs (java.io.File. prefix))

(defn run-flow [command]
  (-> command
    (cascading/generate-flow)
    (.complete)))

(defn run-flow->output [harness command]
  (let [output-file (t/file harness)]
    (->> command
      (pig/store-clj output-file)
      (run-flow))
    (->> output-file
      (t/read harness)
      (map read-string))))

(def-functional-tests "cascading"
  (reify TestHarness
    (data [this data]
      (let [input-file (t/file this)]
        (spit input-file
              (->> data
                (map prn-str)
                (clojure.string/join)))
        (cascading/load-clj input-file)))
    (dump [this command]
      (if (-> command :type #{:store :script})
        (run-flow this command)
        (run-flow->output this command)))
    (file [this]
      (str prefix (gensym)))
    (read [this file]
      (apply concat
        (for [f (io/list-files file)
              :when (not (.endsWith f ".crc"))
              :when (not (.endsWith f "_SUCCESS"))]
          (when-let [contents (not-empty (slurp f))]
            (clojure.string/split-lines contents)))))
    (write [this lines]
      (let [file (t/file this)]
        (spit file (clojure.string/join "\n" lines))
        file)))
  #{
    ;pigpen.functional.code-test/test-closure
    pigpen.functional.code-test/test-for
    ;pigpen.functional.filter-test/test-filter
    ;pigpen.functional.filter-test/test-remove
    pigpen.functional.io-test/test-load-string
    pigpen.functional.io-test/test-load-tsv
    pigpen.functional.io-test/test-load-tsv-non-tab
    pigpen.functional.io-test/test-load-tsv-non-tab-with-tabs
    pigpen.functional.io-test/test-load-clj
    pigpen.functional.io-test/test-load-json
    pigpen.functional.io-test/test-load-json-no-options
    pigpen.functional.io-test/test-load-json-two-options
    pigpen.functional.io-test/test-load-lazy
    pigpen.functional.io-test/test-store-string
    pigpen.functional.io-test/test-store-tsv
    pigpen.functional.io-test/test-store-clj
    pigpen.functional.io-test/test-store-json
    ;pigpen.functional.join-test/test-group-by
    pigpen.functional.join-test/test-into
    pigpen.functional.join-test/test-into-empty
    pigpen.functional.join-test/test-reduce-conj
    pigpen.functional.join-test/test-reduce-+
    pigpen.functional.join-test/test-reduce-empty
    pigpen.functional.join-test/test-fold-inline-sum
    pigpen.functional.join-test/test-fold-inline-count
    pigpen.functional.join-test/test-fold-count
    pigpen.functional.join-test/test-fold-cogroup-dual
    pigpen.functional.join-test/test-fold-all-sum
    pigpen.functional.join-test/test-fold-all-count
    pigpen.functional.join-test/test-fold-all-empty
    pigpen.functional.join-test/test-cogroup-inner
    pigpen.functional.join-test/test-cogroup-left-outer
    pigpen.functional.join-test/test-cogroup-right-outer
    pigpen.functional.join-test/test-cogroup-full-outer
    pigpen.functional.join-test/test-cogroup-inner-join-nils
    pigpen.functional.join-test/test-cogroup-left-outer-join-nils
    pigpen.functional.join-test/test-cogroup-right-outer-join-nils
    pigpen.functional.join-test/test-cogroup-full-outer-join-nils
    pigpen.functional.join-test/test-cogroup-self-join
    pigpen.functional.join-test/test-cogroup-self-join+fold
    pigpen.functional.join-test/test-join-inner-implicit
    pigpen.functional.join-test/test-join-inner
    pigpen.functional.join-test/test-join-left-outer
    pigpen.functional.join-test/test-join-right-outer
    pigpen.functional.join-test/test-join-full-outer
    ;pigpen.functional.join-test/test-join-inner-join-nils
    ;pigpen.functional.join-test/test-join-left-outer-join-nils
    ;pigpen.functional.join-test/test-join-right-outer-join-nils
    ;pigpen.functional.join-test/test-join-full-outer-join-nils
    pigpen.functional.join-test/test-join-self-join
    ;pigpen.functional.join-test/test-join-default-key-selector
    ;pigpen.functional.join-test/test-filter-by
    ;pigpen.functional.join-test/test-filter-by-nil-keys
    ;pigpen.functional.join-test/test-filter-by-duplicate-keys
    pigpen.functional.join-test/test-remove-by
    pigpen.functional.join-test/test-remove-by-nil-keys
    pigpen.functional.join-test/test-remove-by-duplicate-keys
    ;pigpen.functional.map-test/test-map
    ;pigpen.functional.map-test/test-mapcat
    pigpen.functional.map-test/test-map-indexed
    pigpen.functional.map-test/test-map-indexed+sort
    pigpen.functional.map-test/test-sort
    pigpen.functional.map-test/test-sort-desc
    pigpen.functional.map-test/test-sort-by
    pigpen.functional.map-test/test-sort-by-desc
    pigpen.functional.map-test/test-sort-by-with-duplicates
    pigpen.functional.map-test/test-map+fold1
    pigpen.functional.map-test/test-map+fold2
    pigpen.functional.set-test/test-concat
    pigpen.functional.set-test/test-union
    pigpen.functional.set-test/test-union-multiset
    pigpen.functional.set-test/test-intersection
    pigpen.functional.set-test/test-intersection-multiset
    pigpen.functional.set-test/test-difference
    pigpen.functional.set-test/test-difference-multiset
    })
