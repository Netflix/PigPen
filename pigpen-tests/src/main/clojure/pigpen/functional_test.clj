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

(ns pigpen.functional-test
  (:refer-clojure :exclude [read]))

(defprotocol TestHarness
  (data
    [this data]
    "Create mock input with specified data")
  (dump
    [this command]
    "Execute the command & return the results")
  (file
    [this]
    "Returns a new unique filename for writing results to")
  (read
    [this file]
    "Reads data from a file, returns a sequence of lines")
  (write
    [this lines]
    "Writes data to a file, returns the name of the file"))

(defonce all-tests (atom []))

(defmacro deftest
  "Looks like defn, but defines a cross-platform test. Should take a single arg,
which is a TestHarness. This should be used for all data creation and test
execution."
  [name docstring params & body]
  {:pre [(symbol? name)
         (string? docstring)
         (vector? params) (nil? (next params))
         body]}
  `(do
     (swap! all-tests conj '~(symbol (str (ns-name *ns*)) (str name)))
     (defn ~(symbol (clojure.core/name name))
       ~docstring
       ~params
       ~@body)))
