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

(ns pigpen.rx.core-test
  (:require [clojure.test :refer :all]
            [pigpen.rx :as rx]
            [pigpen.raw :as raw]
            [pigpen.core :as pig]
            [pigpen.local :as local :refer [PigPenLocalLoader]]))

(defmethod local/load :bad-storage [command]
  (let [fail (get-in command [:opts :fail])]
    (reify PigPenLocalLoader
      (locations [_]
        (if (= fail :locations)
          (throw (Exception. "locations"))
          ["foo" "bar"]))
      (init-reader [_ _]
        (if (= fail :init-reader)
          (throw (Exception. "init-reader"))
          :reader))
      (read [_ _]
        (if (= fail :read)
          (throw (Exception. "read"))
          [{'value 1}
           {'value 2}
           {'value 3}]))
      (close-reader [_ _]
        (when (= fail :close)
          (throw (Exception. "close-reader")))))))

(deftest test-load-exception-handling
  (testing "normal"
    (let [command (raw/load$ "nothing" :bad-storage ['value] {:fail nil})]
      (is (= (rx/dump command) [1 2 3 1 2 3]))))
  (testing "fail locations"
    (let [command (raw/load$ "nothing" :bad-storage ['value] {:fail :locations})]
      (is (thrown? Exception (rx/dump command)))))
  (testing "fail init-reader"
    (let [command (raw/load$ "nothing" :bad-storage ['value] {:fail :init-reader})]
      (is (thrown? Exception (rx/dump command)))))
  (testing "fail read"
    (let [command (raw/load$ "nothing" :bad-storage ['value] {:fail :read})]
      (is (thrown? Exception (rx/dump command)))))
  (testing "fail close"
    (let [command (raw/load$ "nothing" :bad-storage ['value] {:fail :close})]
      (is (thrown? Exception (rx/dump command))))))

(deftest test-exception-handling
  (let [data (pig/return [1 2 3])
        command (pig/map (fn [x] (throw (java.lang.Exception.))) data)]
    (is (thrown? Exception (rx/dump command)))))
