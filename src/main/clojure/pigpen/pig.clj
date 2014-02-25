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
            [clojure.core.async :as a]
            [clj-time.format :as time]
            [instaparse.core :as insta]
            [taoensso.nippy :refer [freeze thaw]]
            [pigpen.util :as util])
  (:import [pigpen PigPenException]
           [org.apache.pig.data
            DataByteArray
            Tuple TupleFactory
            DataBag BagFactory]
           [java.util List Map]
           [clojure.lang Keyword IPersistentVector]
           [clojure.core.async.impl.protocols Channel]))

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

(defmethod hybrid->clojure Channel [value]
  (->> value util/safe-<!! (map hybrid->clojure)))

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
  (->> value (.getAll) (mapv thaw-values)))

(defmethod thaw-values DataBag [^DataBag value]
  (->> value (.iterator) iterator-seq (map thaw-values)))

;; **********

(def ^:private eval-string
  "Reads code from a string & evaluates it"
  (memoize #(eval (read-string %))))

(defmacro with-ns
  "Evaluates f within ns. Calls (require 'ns) first."
  [ns f]
  `(do
     (require '~ns)
     (binding [*ns* (find-ns '~ns)]
       (eval '~f))))

(defn eval-udf
  [init func ^Tuple t]
  "Evaluates a pig tuple as a clojure function. The first element of the tuple
   is any initialization code. The second element is the function to be called.
   Any remaining args are passed to the function as a collection."
  (try
    (let [args (.getAll t)]
      (when (not-empty init) (eval-string init))
      ((eval-string func) args))
    ;; Errors (like AssertionError) hang the interop layer.
    ;; This allows any problem with user code to pass through.
    (catch Throwable z (throw (PigPenException. z)))))

(defn ^:private bag->chan
  "Takes a bag & pushes the values to a channel. Blocks until the values are consumed."
  [ch ^DataBag bag]
  ;; We flatten tuples in bags as they only ever have a single value
  ;; This matches the behavior of hybrid->clojure
  (doseq [^Tuple t (-> bag (.iterator) iterator-seq)
          value (.getAll t)]
    (util/safe->!! ch value)))

(defn ^:private lazy-bag-args
  "Takes a seq of args. Returns two arg vectors. The first is new arguments,
replacing bags with channel-based lazy bags. The second is the channels
corresponding to the new lazy bags. The second will contain nils for any non-bag
args."
  [args]
  (->> args
    (map (fn [a] (if (instance? DataBag a)
                   ;; TODO tune this
                   (let [c (a/chan java.lang.Long/MAX_VALUE)]
                     [(util/safe-go (util/chan->lazy-seq c)) c])
                   [a nil])))
    (apply map vector)))

(defn ^:private create-accumulate-state
  "Creates a new accumulator state. This is a vector with two elements. The
first is the channels to pass future values to. The second is a channel
containing the singe value of the result."
  [init func ^Tuple tuple]
  (let [args (.getAll tuple)]
    ;; Run init code if present
    (when (not-empty init)
      (eval-string init))
    ;; Make new lazy bags & create a result channel
    (let [[args* input-bags] (lazy-bag-args args)
          ;; Start result evaluation asynchronously, it will block on lazy bags
          result (util/safe-go ((eval-string func) args*))]      
      [input-bags result])))

(defn udf-accumulate
  "Evaluates a pig tuple as a clojure function. The first element of the tuple
is any initialization code. The second element is the function to be called.
Any remaining args are passed to the function as a collection.

This makes use of the Pig Accumulator interface to gradually consume bags. Each
bag argument is converted into a lazy seq via a core.async channel. This
function returns the state of the accumulation - a tuple of two elements. The
first is a vector of channels corresponding to each bag argument. The second is
a channel with the single value result.

This is intended to be called multiple times. For the first call, the first arg,
state, should be nil. On subsequent calls, pass the value returned by this
function as the state. Each subsequent call is expected to have identical args
except for bag, which will contain new values. Non-bag values are ignored and
the bag values are pushed into their respective channels."
  [init func [input-bags result] ^Tuple tuple]
  (try
    (if-not result ; have we started processing this value yet?

      ;; create new channels
      (let [state (create-accumulate-state init func tuple)]
        (udf-accumulate init func state tuple) ;; actually push the initial values
        state)

      ;; push values to existing channels
      (let [args (.getAll tuple)]
        (doall
          (map (fn [input-bag arg]
                 (when input-bag ; arg was a bag
                   (bag->chan input-bag arg)))
               input-bags args))
        [input-bags result]))

    ;; Errors (like AssertionError) hang the interop layer.
    ;; This allows any problem with user code to pass through.
    (catch Throwable z (throw (PigPenException. z)))))

(defn udf-get-value
  "Returns the result value of an accumulation. Closes each input bag and
returns the single value in the result channel."
  [[input-bags result]]
  {:pre [(util/channel? result)]}
  (doseq [b input-bags
          :when b]
    (a/close! b))
  (util/safe-<!! result))

(defn udf-cleanup
  "Cleans up any accumulator state by closing the result channel. Returns nil
as the initial state for the next accumulation."
  [[input-bags result]]
  {:post [(nil? %)]}
  (try
    (when result
      (a/close! result))
    (catch Throwable z (throw (RuntimeException. z)))))

;; **********

(defn ^:private pig-freeze [value]
  (DataByteArray. (freeze value {:skip-header? true, :legacy-mode true})))

(defn ^:private pig-freeze-with-nils [value]
  (if value
    (pig-freeze value)))

(defn args->map
  "Returns a fn that converts a list of args into a map of named parameter
   values. Applies f to all the values."
  [f]
  (fn [& args]
    (->> args
      (partition 2)
      (map (fn [[k v]] [(keyword k) (f v)]))
      (into {}))))

(defn debug [& args]
  "Creates a debug string for the tuple"
  (try
    (->> args (mapcat (juxt type str)) (string/join "\t"))
    (catch Exception z (str "Error getting value: " z))))

;; **********

(defn map->bind
  "Wraps a map function so that it can be consumed by PigPen"
  [f]
  (fn [args]
    [[(apply f args)]])) ;; wrap twice - single value, single arg to next fn

(defn mapcat->bind
  "Wraps a mapcat function so that it can be consumed by PigPen"
  [f]
  (fn [args]
    (map vector (apply f args)))) ;; wrap each value as arg to next fn

(defn filter->bind
  "Wraps a filter function so that it can be consumed by PigPen"
  [f]
  (fn [args]
    (if-let [result (apply f args)]
      [args] ;; wrap as arg to next fn
      [])))

(defn key-selector->bind
  "Returns a tuple of applying f to args and the first arg."
  [f]
  (fn [args]
    [[(apply f args) (first args)]])) ;; wrap twice - single value, two args to next fn

(defn keyword-field-selector->bind
  "Selects a set of fields from a map and projects them as Pig fields. Takes a
single arg, which is a map with keyword keys."
  [fields]
  (fn [args]
    (let [values (first args)]
      [(map values fields)])))

(defn indexed-field-selector->bind
  "Selects the first n fields and projects them as Pig fields. Takes a
single arg, which is sequential. Applies f to the remaining args."
  [n f]
  (fn [args]
    (let [values (first args)]
      [(concat
         (take n values)
         [(f (drop n values))])])))

(defn pre-process
  "Optionally deserializes incoming data"
  [type]
  (fn [args]
    [(for [value args]
       (case type
         :frozen (hybrid->clojure value)
         :native value))]))

(defn post-process
  "Serializes outgoing data"
  [type]
  (fn [args]
    (if (= type :sort)
      (let [[key value] args]
        [[key (pig-freeze value)]])
      [(for [value args]
         (case type
           :frozen (pig-freeze value)
           :frozen-with-nils (pig-freeze-with-nils value)
           :native value))])))

(defn exec
  "Applies the composition of fs, flattening intermediate results. Each f must
produce a seq-able output that is flattened as input to the next command. The
result is wrapped in a tuple and bag."
  [fs]
  (fn [args]
    (->>
      (reduce (fn [vs f] (mapcat f vs)) [args] fs)
      (map (partial apply tuple))
      (apply bag))))

;; TODO lots of duplication here
(defn exec-initial
  "Special exec function for fold. Input will always be a frozen bag. Returns a single frozen value in a tuple."
  [pre seed reducef args]
  (->> args
    first
    hybrid->clojure
    pre
    (reduce reducef seed)
    pig-freeze
    tuple))

(defn exec-intermed
  "Special exec function for fold. Input will always be a frozen bag. Returns a single frozen value in a tuple."
  [combinef args]
  (->> args
    first
    hybrid->clojure
    (reduce combinef)
    pig-freeze
    tuple))

(defn exec-final
  "Special exec function for fold. Input will always be a frozen bag. Returns a single frozen value."
  [combinef post args]
  (->> args
    first
    hybrid->clojure
    (reduce combinef)
    post
    pig-freeze))

(defn udf-algebraic
  "Evaluates an algebraic function. An algebraic function has three stages: the
initial reduce, a combiner, and a final stage. In PigPen, the final stage is
equivalent to the combine stage."
  [init foldf type ^Tuple t]
  (try
    (let [args (.getAll t)]
      (if (not-empty init) (eval-string init))
      (let [{:keys [pre combinef reducef post]} (eval-string foldf)]
        (case type
          :initial
          (exec-initial pre (combinef) reducef args)

          :intermed
          (exec-intermed combinef args)
    
          :final
          (exec-final combinef post args)
    
          :exec
          (->> args
            (exec-initial pre (combinef) reducef)
            ((comp vector bag))
            (exec-intermed combinef)
            ((comp vector bag))
            (exec-final combinef post)))))
    
    (catch Throwable z (throw (PigPenException. z)))))
