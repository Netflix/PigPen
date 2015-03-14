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

(ns pigpen.pig.avro-test
  (:require [clojure.test :refer :all]
            [pigpen.extensions.test :refer [pigsym-inc]]
            [pigpen.pig :as pig]
            [pigpen.avro :as avro]))

(deftest test-avro-script
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (is (= (->>
             (avro/load-avro "resources/example_data.avro" (slurp "resources/example_schema.avsc"))
             (pig/generate-script))
           "REGISTER pigpen.jar;

REGISTER piggybank.jar;

load1 = LOAD 'resources/example_data.avro'
    USING org.apache.pig.piggybank.storage.avro.AvroStorage('schema', '{\"type\":\"record\",\"name\":\"ExampleRecord\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"browserTimestamp\",\"type\":\"long\"}]}');

DEFINE udf5 pigpen.PigPenFnDataBag('(clojure.core/require (quote [pigpen.runtime]) (quote [pigpen.avro.core]))','(pigpen.runtime/exec [(pigpen.runtime/process->bind (pigpen.runtime/pre-process :pig :native)) (pigpen.runtime/map->bind (comp pigpen.avro.core/dotted-keys->nested-map (pigpen.runtime/args->map pigpen.runtime/native->clojure))) (pigpen.runtime/process->bind (pigpen.runtime/post-process :pig :frozen))])');

project3_0 = FOREACH load1 GENERATE
    udf5('browserTimestamp', browserTimestamp) AS (value0);

project3 = FOREACH project3_0 GENERATE
    FLATTEN(value0) AS (value);

"))))
