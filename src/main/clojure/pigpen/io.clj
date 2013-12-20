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

(ns pigpen.io
  "Commands to load, store, and mock data.

  Note: Most of these are present in pigpen.core. Normally you should use those instead.
"
  (:refer-clojure :exclude [constantly])
  (:require [pigpen.raw :as raw]
            [pigpen.code :as code]
            [pigpen.pig :as pig]))

(defmacro load-binary
  "Loads data stored in the pigpen binary format. This is generally not used
unless debugging scripts."
  ([location] `(load-binary ~location [~'value]))
  ([location fields]
    `(raw/load$ ~location '~fields raw/default-storage {})))

(defmacro load-pig
  "Loads data stored in Pig format and converts it to the equivalent Clojure
data structures. The data is a tab-delimited file. 'fields' defines the name for
each input field. The data is returned as a map with 'fields' as the keys.

  Example:

    (pig/load-pig \"input.pig\")

  Note: This is extremely slow. Don't use it.

  See also: pigpen.core/load-tsv, pigpen.core/load-clj
"
  [location fields]
  `(->
     (raw/load$ ~location '~fields raw/default-storage {:cast "chararray"})
     (raw/bind$ '(pigpen.pig/map->bind (pigpen.pig/args->map pigpen.pig/parse-pig))
                {:args '~(clojure.core/mapcat (juxt str identity) fields)
                 :field-type-in :native})))

(defmacro load-clj
  "Loads clojure data from a file. Each line should contain one value and will
be parsed using clojure.edn/read-string into a value.

  Example:

    (pig/load-clj \"input.clj\")

  See also: pigpen.core/load-tsv
"
  [location]
  `(->
     (raw/load$ ~location '~['value] raw/default-storage {:cast "chararray"})
     (raw/bind$ '(pigpen.pig/map->bind clojure.edn/read-string)
                {:requires '[clojure.edn], :field-type-in :native})))

(defn load-tsv
  "Loads data from a tsv file. Each line is returned as a vector of strings,
split by the specified regex delimiter. The default delimiter is #\"\\t\".

  Example:

    (pig/load-tsv \"input.tsv\")
    (pig/load-tsv \"input.tsv\" #\",\")

  Note: Internally this uses \\u0000 as the split char so Pig won't split the line.
        This won't work for files that actually have that char

  See also: pigpen.core/load-clj
"
  ([location] (load-tsv location #"\t"))
  ([location delimiter]
    (->
      (raw/load$ location ['value] (raw/storage$ [] "PigStorage" ["\\u0000"]) {:cast "chararray"})
      (raw/bind$ `(pigpen.pig/map->bind (fn [~'s] (if ~'s (clojure.string/split ~'s ~delimiter))))
                 {:field-type-in :native}))))

;; TODO fix the regex inversion
(defn load-lazy
  "Loads data from a tsv file. Each line is returned as a lazy seq, split by
the specified delimiter. The default delimiter is \\t.

  Note: The delimiter is wrapped with [^ ]+ to negate it for use with re-seq.
        Thus, only simple delimiters are supported. Experimental & might not work.

  Note: Internally this uses \\u0000 as the split char so Pig won't split the line.
        This won't work for files that actually have that char

  See also: pigpen.core/load-tsv
"
  ([location] (load-lazy location #"\t"))
  ([location delimiter]
    (let [delimiter (java.util.regex.Pattern/compile (str "[^" delimiter "]"))]
      (->
        (raw/load$ location ['value] (raw/storage$ [] "PigStorage" ["\\u0000"]) {:cast "chararray"})
        (raw/bind$ `(pigpen.pig/map->bind (fn [~'s] (re-seq ~delimiter ~'s)))
                   {:field-type-in :native})))))

(defn store-binary
  "Stores data in the PigPen binary format. This is generally not used
unless debugging scripts."
  [location relation]
  (raw/store$ relation location raw/default-storage {}))

(defmacro store-pig
  "Stores the relation into location as Pig formatted data.

  Example:

    (pig/store-pig \"output.pig\" foo)

  Note: Pig formatted data is not idempotent. Don't use this.

  See also: pigpen.core/store-clj, pigpen.core/store-tsv
"
  [location relation]
  `(-> ~relation
     (raw/bind$ '(pigpen.pig/map->bind (comp pigpen.pig/pig->string pigpen.pig/hybrid->pig))
                {:args (:fields ~relation), :field-type-out :native})
     (raw/store$ ~location raw/default-storage {})))

(defmacro store-clj
  "Stores the relation into location using edn (clojure format). Each value is
written as a single line.

  Example:

    (pig/store-clj \"output.tsv\" foo)

  See also: pigpen.core/store-tsv

  See: https://github.com/edn-format/edn
"
  [location relation]
  `(-> ~relation
     (raw/bind$ `(pigpen.pig/map->bind pr-str)
                {:args (:fields ~relation), :field-type-out :native})
     (raw/store$ ~location raw/default-storage {})))

(defn store-tsv
  "Stores the relation into location as a tab-delimited file. Thus, each input
value must be sequential. Complex values are stored as edn (clojure format).
Single string values are not quoted. You may optionally pass a different delimiter.

  Example:

    (pig/store-tsv \"output.tsv\" foo)
    (pig/store-tsv \"output.csv\" \",\" foo)

  See also: pigpen.core/store-clj

  See: https://github.com/edn-format/edn
"
  ([location relation] (store-tsv location "\t" relation))
  ([location delimiter relation]
    (-> relation
      (raw/bind$ `(pigpen.pig/map->bind (fn [~'s] (clojure.string/join ~delimiter (map print-str ~'s))))
                 {:args (:fields relation), :field-type-out :native})
      (raw/store$ location raw/default-storage {}))))

(defn return
  "Returns a constant set of data as a pigpen relation. This is useful for
testing, but not supported in generated scripts. The parameter 'data' must be a
sequence. The values of 'data' can be any clojure type.

  Example:

    (pig/constantly [1 2 3])
    (pig/constantly [{:a 123} {:b 456}])

  See also: pigpen.core/constantly
"
  [data]
  (raw/return$
    (for [d data]
      (pig/freeze-vals {'value d}))))

(defn return-raw
  "Returns a constant set of data for script debugging and testing.
For internal use only."
  [data] (raw/return$ data))

(defn constantly
  "Returns a function that takes any number of arguments and returns a constant
set of data as if it had been loaded by pigpen. This is useful for testing,
but not supported in generated scripts. The parameter 'data' must be a sequence.
The values of 'data' can be any clojure type.

  Example:

    (pig/constantly [1 2 3])
    (pig/constantly [{:a 123} {:b 456}])

  See also: pigpen.core/return
"
  [data]
  (clojure.core/constantly
    (return data)))
