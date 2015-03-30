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

(set! *warn-on-reflection* true)

(ns pigpen.extensions.io
  (:import [java.io File]))

(defn list-files
  "List files in a folder"
  [dir]
  (if (string? dir)
    (map #(.getAbsolutePath ^File %)
         (list-files (File. ^String dir)))
    (when (.exists ^File dir)
      (if (.isDirectory ^File dir)
        (mapcat list-files (.listFiles ^File dir))
        (when-not (.startsWith (.getName ^File dir) ".")
          [dir])))))

(defn clean
  "Recursively delete files & folders from the path specified"
  [out-dir]
  (if (string? out-dir)
    (clean (File. ^String out-dir))
    (when (.exists ^File out-dir)
      (when (.isDirectory ^File out-dir)
        (doseq [file (.listFiles ^File out-dir)]
          (clean file)))
      (.delete ^File out-dir))))
