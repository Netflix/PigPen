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

(ns pigpen.pig
  "Contains functions that are used when interacting with Pig. This includes
mapping of data from Pig to Clojure and back, serializing and deserializing
data, and executing user code. Everything in here should be as performant as
possible as it's used at runtime."
  (:require [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clj-time.format :as time]
            [instaparse.core :as insta]
            [taoensso.nippy :refer [freeze thaw]])
  (:import [pigpen PigPenException]
           [org.apache.pig.data
            DataByteArray
            Tuple TupleFactory
            DataBag BagFactory]
           [java.util List Map]
           [clojure.lang Keyword IPersistentVector]))

(set! *warn-on-reflection* true)

(defn ^Tuple tuple
  "Create a pig tuple"
  [& vals]
  (if vals
    (.newTupleNoCopy (TupleFactory/getInstance) vals)
    (.newTuple (TupleFactory/getInstance))))

(defn ^DataBag bag
  "Create a pig bag"
  [& vals]
  {:pre [(every? (partial instance? Tuple) vals)]}
  (if vals
    (.newDefaultBag (BagFactory/getInstance) vals)
    (.newDefaultBag (BagFactory/getInstance))))

;; **********

(defn byte->hex-digit
  "Converts an ascii char into its hex value"
  [b]
  ;; See ascii table for magic numbers
  (if (<= b 57) (- b 48) (- b 87)))

(defn bytes->string->bytes
  "More efficient version of (comp pig/string->bytes pig/bytes->string)"
  [value]
  (->> value
    (map byte->hex-digit)
    (partition 2)
    (map (fn [[i1 i0]] (unchecked-byte (+ (* i1 16) i0))))
    (byte-array)))

(defn string->bytes
  "Converts a pig string representation of bytes into an actual byte array.

   For example: \"303132\" > [0x30 0x31 0x32]
"
  [value]
  (->> value
    (partition 2)
    (map (partial apply str))
    (map #(Long/parseLong % 16))
    (map unchecked-byte)
    (byte-array)))

(defn string->DataByteArray
  "Converts a string representation of bytes into what pig produces.

   For example: \"303132\" [0x33 0x30 0x33 0x31 0x33 0x32]
"
  [value]
  (->> value
    (map byte)
    (byte-array)
    (bytes)
    (DataByteArray.)))

(defn bytes->int
  "Convert bytes into an int"
  [b]
  (if b (.getInt (java.nio.ByteBuffer/wrap b))))

(defn bytes->long
  "Convert bytes into a long"
  [b]
  (if b (.getLong (java.nio.ByteBuffer/wrap b))))

(defn bytes->string
  "Convert bytes into a string"
  [b]
  (if b (String. (bytes b))))

(defn bytes->debug
  "Convert bytes into a debug string"
  [b]
  (clojure.string/join (mapv #(format "%02X" %) b)))

(defn bytes->json
  "Convert bytes into a string & then parses json"
  [b]
  (if b (json/read-json (String. (bytes b)))))

(defn cast-bytes
  "Converts a byte array into the specified type. Similar to a cast in pig."
  [type value]
  ((case type
     nil identity
     "bytearray" identity
     "int" bytes->int
     "long" bytes->long
     "chararray" bytes->string)
    value))

;; **********

(def ^:private string->pig
  (insta/parser
    "
<PIG>     = TUPLE | BAG | MAP | LITERAL
TUPLE     = <'()'> | <'('> PIG (<','> PIG)* <')'>
BAG       = <'{}'> | <'{'> TUPLE (<','> TUPLE)* <'}'>
MAP       = <'[]'> | <'['> MAP-ENTRY (<','> MAP-ENTRY)* <']'>
MAP-ENTRY = STRING <'#'> PIG
<LITERAL> = (DATETIME | NUMBER | BOOLEAN) / STRING
DATETIME  = #'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}\\+\\d{2}:\\d{2}'
NUMBER    = #'-?(\\d+\\.\\d+|[1-9][0-9]*|0)(e\\d+)?' <('l' | 'L' | 'f' | 'F')?>
BOOLEAN   = 'true' | 'TRUE' | 'false' | 'FALSE'
STRING    = #'[^\\,\\)\\}\\]\\#]+'
"))

(defmulti ^:private pig->clojure first)

(defmethod pig->clojure :STRING [[_ v]] v)

(defmethod pig->clojure :BOOLEAN [[_ v]] 
  (-> v clojure.string/lower-case read-string))

(defmethod pig->clojure :NUMBER [[_ v]]
  (read-string v))

(defmethod pig->clojure :DATETIME [[_ v]]
  (time/parse v))

(defmethod pig->clojure :MAP-ENTRY [[_ k v]]
  [(pig->clojure k) (pig->clojure v)])

(defmethod pig->clojure :MAP [[_ & entries]]
  (into {} (map #(pig->clojure %) entries)))

(defmethod pig->clojure :BAG [[_ & items]]
  (into [] (map #(pig->clojure %) items)))

(defmethod pig->clojure :TUPLE [[_ & items]]
  (into [] (map #(pig->clojure %) items)))

(defn parse-pig [data]
  (let [parsed (string->pig data)]
    (if (insta/failure? parsed)
      (insta/get-failure parsed)
      (pig->clojure (first parsed)))))

;; **********

(defmulti hybrid->pig
  "Converts a hybrid pig/clojure data structure into 100% pig.

   DataByteArrays are assumed to be frozen clojure structures."
  type)

(defmethod hybrid->pig nil [value]
  value)

(defmethod hybrid->pig String [value]
  value)

(defmethod hybrid->pig Number [value]
  value)

(defmethod hybrid->pig Boolean [value]
  value)

(defmethod hybrid->pig Keyword [value]
  (name value))

(defmethod hybrid->pig List [value]
  (->> value
    (map hybrid->pig)
    (apply tuple)))

(defmethod hybrid->pig Map [value]
  (->> value
    (map (fn [[k v]] [(hybrid->pig k) (hybrid->pig v)]))
    (into {})))

(defmethod hybrid->pig DataByteArray [^DataByteArray value]
  (->> value (.get) thaw hybrid->pig))

(defmethod hybrid->pig Tuple [^Tuple value]
  (->> value (.getAll) (map hybrid->pig) (apply tuple)))

(defmethod hybrid->pig DataBag [^DataBag value]
  (->> value (.iterator) iterator-seq (map hybrid->pig) (apply bag)))

;; **********

(defmulti pig->string
  "Serializes pig data structures into a string"
  type)

(defmethod pig->string :default [value]
  (str value))

(defmethod pig->string String [value]
  value)

(defmethod pig->string Map [value]
  (as-> value %
    (map (fn [[k v]] (str (pig->string k) "#" (pig->string v))) %)
    (string/join "," %)
    (str "[" % "]")))

(defmethod pig->string DataByteArray [^DataByteArray value]
  (bytes->debug (.get value)))

;; **********

(defmulti hybrid->clojure
  "Converts a hybrid pig/clojure data structure into 100% clojure.

   DataByteArrays are assumed to be frozen clojure structures.

   Only raw pig types are expected here - anything normal (Boolean, String, etc)
   should be frozen."
  type)

(defmethod hybrid->clojure :default [value]
  (throw (IllegalStateException. (str "Unexpected value:" value))))

(defmethod hybrid->clojure nil [value]
  ;; nils are allowed because they occur in joins
  nil)

(defmethod hybrid->clojure Number [value]
  ;; Numbers are allowed because RANK produces them
  value)

(defmethod hybrid->clojure DataByteArray [^DataByteArray value]
  (-> value (.get) thaw))

(defmethod hybrid->clojure Tuple [^Tuple value]
  (->> value (.getAll) (mapv hybrid->clojure)))

(defmethod hybrid->clojure DataBag [^DataBag value]
  ;; This is flattened to help with dereferenced fields that result in a bag of a single tuple
  (->> value (.iterator) iterator-seq (mapcat hybrid->clojure)))

;; **********

(defmulti native->clojure
  "Converts native pig data structures into 100% clojure.

   No clojure should be seen here.

   DataByteArrays are converted to byte arrays."
  type)

(defmethod native->clojure nil [value]
  value)

(defmethod native->clojure String [value]
  value)

(defmethod native->clojure Number [value]
  value)

(defmethod native->clojure Boolean [value]
  value)

(defmethod native->clojure Map [value]
  (->> value
    (map (fn [[k v]] [(native->clojure k) (native->clojure v)]))
    (into {})))

(defmethod native->clojure DataByteArray [^DataByteArray value]
  (.get value))

(defmethod native->clojure Tuple [^Tuple value]
  (->> value (.getAll) (mapv native->clojure)))

(defmethod native->clojure DataBag [^DataBag value]
  (->> value (.iterator) iterator-seq (map native->clojure)))

;; **********

(defn freeze-vals [value]
  {:pre [(map? value)]}
  (reduce-kv (fn [m k v] (assoc m k (DataByteArray. (freeze v)))) {} value))

(defmulti thaw-anything
  "Attempts to thaw any child value. Returns it as '(freeze ...)"
  type)

(defmethod thaw-anything :default [value]
  value)

(defmethod thaw-anything IPersistentVector [value]
  (mapv thaw-anything value))

(defmethod thaw-anything List [value]
  (map thaw-anything value))

(prefer-method thaw-anything IPersistentVector List)

(defmethod thaw-anything Map [value]
  (->> value
    (map (fn [[k v]] [(thaw-anything k) (thaw-anything v)]))
    (into {})))

(defmethod thaw-anything DataByteArray [^DataByteArray value]
  `(~(symbol "freeze") ~(thaw (.get value))))

(defmethod thaw-anything Tuple [^Tuple value]
  (let [v (->> value (.getAll) (mapv thaw-anything))]
    `(~(symbol "tuple") ~@v)))

(defmethod thaw-anything DataBag [^DataBag value]
  (let [v (->> value (.iterator) iterator-seq (map thaw-anything))]
    `(~(symbol "bag") ~@v)))

(defmulti thaw-values
  "Attempts to thaw any child value. Returns only the value, strips all
serialization info."
  type)

(defmethod thaw-values :default [value]
  value)

(defmethod thaw-values IPersistentVector [value]
  (mapv thaw-values value))

(defmethod thaw-values List [value]
  (map thaw-values value))

(prefer-method thaw-values IPersistentVector List)

(defmethod thaw-values Map [value]
  (->> value
    (map (fn [[k v]] [(thaw-values k) (thaw-values v)]))
    (into {})))

(defmethod thaw-values DataByteArray [^DataByteArray value]
  (thaw (.get value)))

(defmethod thaw-values Tuple [^Tuple value]
  (let [v (->> value (.getAll) (mapv thaw-values))]
    (vec v)))

(defmethod thaw-values DataBag [^DataBag value]
  (let [v (->> value (.iterator) iterator-seq (map thaw-values))]
    (list* v)))

;; **********

(def ^:private eval-string
  "Reads code from a string & evaluates it"
  (memoize #(eval (read-string %))))

(defn eval-udf
  [^Tuple t]
  "Evaluates a pig tuple as a clojure function. The first element of the tuple
   is any initialization code. The second element is the function to be called.
   Any remaining args are passed to the function as a collection."
  (try
    (let [[init func & args] (.getAll t)]
      (when (not-empty init) (eval-string init))
      ((eval-string func) args))
    ;; Errors (like AssertionError) hang the interop layer.
    ;; This allows any problem with user code to pass through.
    (catch Throwable z (throw (PigPenException. z)))))

;; **********

(defn map->bind [f]
  (fn [& args]
    [(apply f args)]))

(defn filter->bind [f]
  (fn [& args]
    (if-let [result (apply f args)]
      args
      [])))

(defn args->map
  "Returns a fn that converts a list of args into a map of named parameter
   values. Applies f to all the values."
  [f]
  (fn [& args]
    (->> args
      (partition 2)
      (map (fn [[k v]] [(keyword k) (f v)]))
      (into {}))))

(defn ^:private pig-freeze [value]
  (DataByteArray. (freeze value)))

(defn ^:private pig-freeze-with-nils [value]
  (if value
    (pig-freeze value)))

(defn ^:private pre-process [type value]
  (case type
    :frozen (hybrid->clojure value)
    :native value))

(defn ^:private post-process [type value]
  (case type
    :frozen (pig-freeze value)
    :frozen-with-nils (pig-freeze-with-nils value)
    :native value))

(defn exec
  "Optionally thaws args, applies f, and optionally freezes the result."
  [field-type-in field-type-out f]
  (fn [args]
    (->> args
      (map (partial pre-process field-type-in))
      (apply f)
      (post-process field-type-out))))

(defn exec-multi
  "Optionally thaws args, applies the composition of fs, flattening
   intermediate results, and optionally freezes the result. Each f must
   produce a seq-able output that is flattened as input to the next command."
  [field-type-in field-type-out fs]
  (fn [args]
    (let [args' (mapv (partial pre-process field-type-in) args)]
      (->>
        (reduce (fn [vs f]
                  (apply concat
                         (for [v vs]
                           (map vector (apply f v))))) [args'] fs)
        ;; TODO why not just call tuple here?
        (map (comp (partial apply tuple)
                   vector
                   (partial post-process field-type-out)
                   first))
        (apply bag)))))

(defn debug [& args]
  "Creates a debug string for the tuple"
  (try
    (->> args (mapcat (juxt type str)) (string/join "\t"))
    (catch Exception z (str "Error getting value: " z))))
