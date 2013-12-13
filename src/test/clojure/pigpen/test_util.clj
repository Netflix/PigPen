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

(ns pigpen.test-util
  (:require [clojure.test :refer [is]]
            [clojure.data :refer [diff]]
            [pigpen.pig :as pig]
            [taoensso.nippy :refer [freeze thaw]])
  (:import [org.apache.pig.data DataByteArray]))

(defn test-diff [actual expected]
  (let [d (diff expected actual)
        expected-only (nth d 0)
        actual-only (nth d 1)]
    (is (= nil expected-only))
    (is (= nil actual-only))))

(defn pigsym-zero [prefix-string]
  (symbol (str prefix-string 0)))

(defn pigsym-inc []
  (let [pigsym-current (atom 0)]
    (fn [prefix-string]
      (symbol (str prefix-string (swap! pigsym-current inc))))))

(def known-functions [[:opts :field-converter]])

(defn verify-functions [command]
  (let [command (reduce (fn [c f] (if (get-in c f) (update-in c f fn?) c)) command known-functions)]
    (if-not (:ancestors command)
      command      
      (update-in command [:ancestors] #(map verify-functions %)))))

(defn regex->string [command]
  ;; regexes don't implement value equality so we make them strings for tests
  (clojure.walk/postwalk #(if (instance? java.util.regex.Pattern %) (str %) %) command))
