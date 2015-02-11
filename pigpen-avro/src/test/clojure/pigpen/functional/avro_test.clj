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

(ns pigpen.functional.avro-test
  (:require [clojure.test :refer :all]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc regex->string]]
            [pigpen.core :as pig]
            [pigpen.fold :as fold]
            [pigpen.avro.core :as pig-avro]))

(.mkdirs (java.io.File. "build/functional/avro-test/test-avro"))

(def clj-data
  [{:browserTimestamp 1417997369042,
     :requestSpan
     {:spanId "2873f846-c867-4e08-8aa8-530f63bbee2b",
      :parentSpanId "0baef555-47ef-48a3-b218-6bfd33094ecd",
      :metadata {:schema_id "7d8d87ef3315c672a28b6ae2d31bd9fa"}},
     :metadata {:schema_id "e48b9786fd4598fa27c1de354f735"},
     :rawHash
     {"queryParams" "",
      "rallyRequestId"
      "qs-app-04144pliuzkfbmu1aiwzrecc81s5.qs-app-049893092",
      "uId" "14125918269",
      "bNo" "15959",
      "sId" "12850"}}
    {:browserTimestamp 1417997368078,
     :requestSpan
     {:spanId "3e313af4-76b8-48cf-abc8-92033f225126",
      :parentSpanId "7673ed70-cfd0-4fb3-9722-33ae8a977301",
      :metadata {:schema_id "7d8d87ef3315c672a28b6ae2d31bd9fa"}},
     :metadata {:schema_id "e48b9786fd4598fa27c1de354f735"},
     :rawHash
     {"queryParams" "",
      "uId" "14125918269",
      "bNo" "15959",
      "sId" "12850"}}
    {:browserTimestamp 1417997392079,
     :requestSpan
     {:spanId "3e313af4-76b8-48cf-abc8-92033f225126",
      :metadata {:schema_id "7d8d87ef3315c672a28b6ae2d31bd9fa"}},
     :metadata {:schema_id "e48b9786fd4598fa27c1de354f735"},
     :rawHash
     {"queryParams" "",
      "uId" "14125918269",
      "bNo" "15959",
      "sId" "12850"}
     :panel {:defOid 29991883477}}
    {
     :browserTimestamp 1417997392079,
     :rawHash
     {"uId" "14125918269",
      "bNo" "15959",
      "queryParams" "",
      "sId" "12850"},
     :metadata {:schema_id "e48b9786fd4598fa27c1de354f735"},
     :requestSpan
     {
      :metadata {:schema_id "7d8d87ef3315c672a28b6ae2d31bd9fa"},
      :handledExceptions
      [
       {:exceptionClass "FooExeption",
        :exceptionMessage "foomessage",
        :exceptionStack "prod"}
       ],
      :spanId "3e313af4-76b8-48cf-abc8-92033f225126",
      },
     :panel {:defOid 29991883477}}])


;; Example data was generated like,
;; $ java -jar ~/avro-tools-1.7.7.jar fromjson --codec snappy \
;;     --schema-file example_schema.avsc example_data.json
(deftest test-avro
  (let [query (pig-avro/load-avro
               "resources/example_data.avro" (slurp "resources/example_schema.avsc"))]
    (is (= (pig/dump query) clj-data))))

(deftest test-fold
  (let [query (->>
               (pig-avro/load-avro
                "resources/example_data.avro" (slurp "resources/example_schema.avsc"))
                            (pig/map #(get % :browserTimestamp 0))
                            (pig/fold (fold/sum)))]
    (is (= (pig/dump query) [5671989521278]))))

(deftest test-compatibility
  (let [query (->>
               (pig-avro/load-avro
                "resources/example_data.avro" (slurp "resources/example_compatibility_new.avsc"))
               (pig/map #(get % :newFieldWithDefault 0))
               (pig/fold (fold/distinct))
               (pigpen.oven/bake))]
    (is (.contains (pig/generate-script query) "\"default\":\"foo\""))
    (is (= (pig/dump query) [#{"foo"}]))))

(comment (run-tests))
