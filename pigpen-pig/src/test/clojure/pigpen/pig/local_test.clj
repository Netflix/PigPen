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
;;     Unless required by apig-locallicable law or agreed to in writing, software
;;     distributed under the License is distributed on an "AS IS" BASIS,
;;     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;     See the License for the specific language governing permissions and
;;     limitations under the License.
;;
;;

(ns pigpen.pig.local-test
  (:require [clojure.test :refer :all]
            [pigpen.pig.runtime :refer [cast-bytes tuple]]
            [pigpen.pig.local :as pig-local]
            [pigpen.hadoop :as hadoop])
  (:import [org.apache.pig.builtin PigStorage]))

(.mkdirs (java.io.File. "build/functional/pig-test"))

(deftest test-load-func->values
  (let [loc "build/functional/pig-test/test-load-func"
        _ (spit loc "1\ta\n2\tb")
        raw-values (pig-local/load-func->values (PigStorage.) {} loc '[x y])]
    (is (= (for [v raw-values
                 [k v] v]
             [k (cast-bytes "chararray" (.get v))])
           '[[y "a"] [x "1"] [y "b"] [x "2"]]))))

(deftest test-store-func->writer
  (let [loc "build/functional/pig-test/test-store-func"
        writer (pig-local/store-func->writer (PigStorage.) "x:chararray,y:chararray" loc)]
    (doseq [value [(tuple "a" "1") (tuple "b" "2")]]
      (hadoop/write-record-writer writer value))
    (hadoop/close-record-writer writer)
    (is (= (slurp (str loc "/part-m-00001"))
           "a\t1\nb\t2\n"))))
