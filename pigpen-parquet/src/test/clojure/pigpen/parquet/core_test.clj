;;
;;
;;  Copyright 2014-2015 Netflix, Inc.
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

(ns pigpen.parquet.core-test
  (:require [clojure.test :refer :all]
            [pigpen.functional-test :as t]
            [pigpen.extensions.test :refer [test-diff]]
            [pigpen.parquet :as pq]
            [pigpen.core :as local])
  (:import [java.io File]))

(def parquet-sample-data
  [{:s "stri\ng"
    :b true
    :i (int 42)
    :l 42
    :f (float 3.14)
    :d 3.14}
   {:s "foo"
    :b false
    :i Integer/MAX_VALUE
    :l Long/MAX_VALUE
    :f Float/MAX_VALUE
    :d Double/MAX_VALUE}])

(def parquet-sample-schema
  (pq/message "sample"
    (pq/binary "s")
    (pq/boolean "b")
    (pq/int32 "i")
    (pq/int64 "l")
    (pq/float "f")
    (pq/double "d")))

;; All platforms are tested against local, including local

(defn load-parquet-data [file]
  (->>
    (pq/load-parquet file parquet-sample-schema)
    (local/dump)))

(defn store-parquet-data [file]
  (->>
    (local/return parquet-sample-data)
    (pq/store-parquet file parquet-sample-schema)
    (local/dump)))

(t/deftest test-load-parquet
  "test loading parquet data"
  [harness]
  (let [file (t/file harness)]
    (store-parquet-data file)
    (is (= (->>
             (pq/load-parquet file parquet-sample-schema)
             (t/dump harness))
           parquet-sample-data))))

(t/deftest test-store-parquet
  "test storing parquet data"
  [harness]
  (let [file (t/file harness)]
    (->>
      (t/data harness parquet-sample-data)
      (pq/store-parquet file parquet-sample-schema)
      (t/dump harness))
    ;; Parquet won't ignore this file, even though it produces it
    (when-let [^File success-file (File. (str file "/_SUCCESS"))]
      (.delete success-file))
    (test-diff
      (load-parquet-data file)
      parquet-sample-data)))
