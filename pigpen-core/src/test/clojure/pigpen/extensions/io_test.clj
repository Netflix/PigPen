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

(ns pigpen.extensions.io-test
  (:require [clojure.test :refer :all]
            [pigpen.extensions.io :refer :all])
  (import [java.io File FileOutputStream]))

(deftest test-list-files
  (.mkdirs (java.io.File. "build/extensions/io-test/test-list-files/"))
  (.close (FileOutputStream. (File. "build/extensions/io-test/test-list-files/foo")))
  (.close (FileOutputStream. (File. "build/extensions/io-test/test-list-files/bar")))
  (let [dir (System/getProperty "user.dir")]
    (is (= (set (list-files "build/extensions/io-test"))
           #{(str dir "/build/extensions/io-test/test-list-files/foo")
             (str dir "/build/extensions/io-test/test-list-files/bar")}))))

(deftest test-clean
  (.mkdirs (java.io.File. "build/extensions/io-test/test-clean/d1/"))
  (.mkdirs (java.io.File. "build/extensions/io-test/test-clean/d2/d3/"))
  (.close (FileOutputStream. (File. "build/extensions/io-test/test-clean/d1/f1")))
  (.close (FileOutputStream. (File. "build/extensions/io-test/test-clean/d2/f2")))
  (.close (FileOutputStream. (File. "build/extensions/io-test/test-clean/d2/d3/f3")))
  (clean "build/extensions/io-test/test-clean")
  (is (empty? (list-files "build/extensions/io-test/test-clean"))))
