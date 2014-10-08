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

(ns pigpen.io-test
  (:use clojure.test)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc regex->string]]
            [pigpen.io :as io]))

(deftest test-load-binary
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/load-binary "foo")
      '{:type :load
        :id load1
        :description "foo"
        :location "foo"
        :fields [value]
        :field-type :native
        :storage :binary
        :opts {:type :load-opts}})))

(deftest test-load-string
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/load-string "foo")
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.runtime/map->bind clojure.core/identity)
        :args [value]
        :requires []
        :fields [value]
        :field-type-in :native
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :load
                     :id load1
                     :description "foo"
                     :location "foo"
                     :fields [value]
                     :field-type :native
                     :storage :string
                     :opts {:type :load-opts}}]})))

(deftest test-load-tsv
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (regex->string (io/load-tsv "foo"))
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.runtime/map->bind (clojure.core/fn [s] (if s (pigpen.extensions.core/structured-split s "\t"))))
        :args [value]
        :requires [pigpen.extensions.core]
        :fields [value]
        :field-type-in :native
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :load
                     :id load1
                     :description "foo"
                     :location "foo"
                     :fields [value]
                     :field-type :native
                     :storage :string
                     :opts {:type :load-opts}}]})))

(deftest test-load-clj
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/load-clj "foo")
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.runtime/map->bind clojure.edn/read-string)
        :args [value]
        :requires [clojure.edn]
        :fields [value]
        :field-type-in :native
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :load
                     :id load1
                     :description "foo"
                     :location "foo"
                     :fields [value]
                     :field-type :native
                     :storage :string
                     :opts {:type :load-opts}}]})))

(deftest test-load-json
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/load-json "foo")
      '{:type :bind
        :id bind2
        :description nil
        :func (pigpen.runtime/map->bind
                (clojure.core/fn [s]
                  (clojure.data.json/read-str s
                                              :key-fn (pigpen.runtime/with-ns pigpen.io-test
                                                        clojure.core/keyword))))
        :args [value]
        :requires [clojure.data.json]
        :fields [value]
        :field-type-in :native
        :field-type-out :frozen
        :opts {:type :bind-opts}
        :ancestors [{:type :load
                     :id load1
                     :description "foo"
                     :location "foo"
                     :fields [value]
                     :field-type :native
                     :storage :string
                     :opts {:type :load-opts}}]})))

(deftest test-load-lazy
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (regex->string (io/load-lazy "foo"))
      '{:type :bind
       :id bind2
       :description nil
       :func (pigpen.runtime/map->bind (clojure.core/fn [s] (pigpen.extensions.core/lazy-split s "\t")))
       :args [value]
       :requires [pigpen.extensions.core]
       :fields [value]
       :field-type-in :native
       :field-type-out :frozen
       :opts {:type :bind-opts}
       :ancestors [{:type :load
                    :id load1
                    :description "foo"
                    :location "foo"
                    :fields [value]
                    :field-type :native
                    :storage :string
                    :opts {:type :load-opts}}]})))

(deftest test-store-binary
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/store-binary "foo" {:fields '[value]})
      '{:type :store
        :id store1
        :description "foo"
        :location "foo"
        :ancestors [{:fields [value]}]
        :fields [value]
        :opts {:type :store-opts}
        :storage :binary})))

(deftest test-store-string
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/store-string "foo" {:fields '[value]})
      '{:type :store
        :id store2
        :description "foo"
        :location "foo"
        :fields [value]
        :opts {:type :store-opts}
        :storage :string
        :ancestors [{:type :bind
                     :id bind1
                     :description nil
                     :ancestors [{:fields [value]}]
                     :func (pigpen.runtime/map->bind clojure.core/str)
                     :args [value]
                     :requires []
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :native
                     :opts {:type :bind-opts}}]})))

(deftest test-store-tsv
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (regex->string (io/store-tsv "foo" {:fields '[value]}))
      '{:type :store
        :id store2
        :description "foo"
        :location "foo"
        :fields [value]
        :opts {:type :store-opts}
        :storage :string
        :ancestors [{:type :bind
                     :id bind1
                     :description nil
                     :ancestors [{:fields [value]}]
                     :func (pigpen.runtime/map->bind (clojure.core/fn [s] (clojure.string/join "\t" (clojure.core/map clojure.core/print-str s))))
                     :args [value]
                     :requires []
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :native
                     :opts {:type :bind-opts}}]})))

(deftest test-store-clj
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/store-clj "foo" {:fields '[value]})
      '{:type :store
        :id store2
        :description "foo"
        :location "foo"
        :fields [value]
        :opts {:type :store-opts}
        :storage :string
        :ancestors [{:type :bind
                     :id bind1
                     :description nil
                     :ancestors [{:fields [value]}]
                     :func (pigpen.runtime/map->bind clojure.core/pr-str)
                     :args [value]
                     :requires []
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :native
                     :opts {:type :bind-opts}}]})))

(deftest test-store-json
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (io/store-json "foo" {:fields '[value]})
      '{:type :store
        :id store2
        :description "foo"
        :location "foo"
        :fields [value]
        :opts {:type :store-opts}
        :storage :string
        :ancestors [{:type :bind
                     :id bind1
                     :description nil
                     :ancestors [{:fields [value]}]
                     :func (pigpen.runtime/map->bind
                             (clojure.core/fn [s]
                               (clojure.data.json/write-str s)))
                     :args [value]
                     :requires [clojure.data.json]
                     :fields [value]
                     :field-type-in :frozen
                     :field-type-out :native
                     :opts {:type :bind-opts}}]})))

(deftest test-return
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      (io/return [1 2 3])
      '{:type :return
        :id return0
        :fields [value]
        :data [{value 1}
               {value 2}
               {value 3}]})))

(deftest test-constantly
  (with-redefs [pigpen.raw/pigsym pigsym-zero]
    (test-diff
      ((io/constantly [1 2 3]))
      '{:type :return
        :id return0
        :fields [value]
        :data [{value 1}
               {value 2}
               {value 3}]})))
