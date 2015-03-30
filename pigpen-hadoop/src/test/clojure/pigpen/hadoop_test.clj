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

(ns pigpen.hadoop-test
  (:require [clojure.test :refer :all]
            [pigpen.hadoop :as hadoop])
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapreduce Job JobContext TaskAttemptContext]
           [org.apache.hadoop.mapreduce.lib.input TextInputFormat]
           [org.apache.hadoop.mapreduce.lib.output TextOutputFormat]))

(deftest test-config
  (instance? Configuration
             (hadoop/config {"foo" "bar"})))

(deftest test-job
  (instance? Job
             (->>
               (hadoop/config {"foo" "bar"})
               (hadoop/job))))

(deftest test-job-context
  (instance? JobContext
             (->>
               (hadoop/config {"foo" "bar"})
               (hadoop/job)
               (hadoop/job-context))))

(deftest test-task-context
  (instance? TaskAttemptContext
             (->>
               (hadoop/config {"foo" "bar"})
               (hadoop/job)
               (hadoop/task-context))))

(.mkdirs (java.io.File. "build/functional/hadoop-test"))

(deftest test-input-format->values
  (let [loc "build/functional/hadoop-test/test-input-format"
        _ (spit loc "foo\nbar\nbaz")
        raw-values (hadoop/input-format->values (TextInputFormat.) {} loc identity)
        values (map #(.toString %) raw-values)]
    (is (= values
           ["foo" "bar" "baz"]))))

(deftest test-output-format->writer
  (let [loc "build/functional/hadoop-test/test-output-format"
        writer (hadoop/output-format->writer (TextOutputFormat.) {} loc)]
    (doseq [value ["foo" "bar" "baz"]]
      (hadoop/write-record-writer writer value))
    (hadoop/close-record-writer writer)
    (is (= (slurp (str loc "/part-m-00001"))
           "foo\nbar\nbaz\n"))))
