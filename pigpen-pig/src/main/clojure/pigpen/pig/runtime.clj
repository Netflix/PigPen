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

(ns pigpen.pig.runtime
  "Contains functions that are used when interacting with Pig. This includes
mapping of data from Pig to Clojure and back, serializing and deserializing
data, and executing user code. Everything in here should be as performant as
possible as it's used at runtime."
  (:require [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clojure.core.async :as a]
            [pigpen.runtime :as rt]
            [pigpen.extensions.core-async :as ae]
            [taoensso.nippy :refer [freeze thaw]])
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

(defn ^:private pig-freeze [value]
  (DataByteArray. (freeze value {:skip-header? true, :legacy-mode true})))

(defn ^:private pig-freeze-with-nils [value]
  (if value
    (pig-freeze value)))

;; **********

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

(defmethod rt/hybrid->clojure DataByteArray [^DataByteArray value]
  (-> value (.get) thaw))

(defmethod rt/hybrid->clojure Tuple [^Tuple value]
  (->> value (.getAll) (mapv rt/hybrid->clojure)))

(defmethod rt/hybrid->clojure DataBag [^DataBag value]
  ;; This is flattened to help with dereferenced fields that result in a bag of a single tuple
  (->> value (.iterator) iterator-seq (mapcat rt/hybrid->clojure)))

(defmethod rt/hybrid->clojure Channel [value]
  (->> value ae/safe-<!! (map rt/hybrid->clojure)))

;; **********

(defmethod rt/native->clojure DataByteArray [^DataByteArray value]
  (.get value))

(defmethod rt/native->clojure Tuple [^Tuple value]
  (->> value (.getAll) (mapv rt/native->clojure)))

(defmethod rt/native->clojure DataBag [^DataBag value]
  (->> value (.iterator) iterator-seq (map rt/native->clojure)))

;; **********

(defn freeze-vals [value]
  {:pre [(map? value)]}
  (reduce-kv (fn [m k v] (assoc m k (pig-freeze v))) {} value))

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

(defn udf-lookup [type]
  (case type
    :scalar  "pigpen.PigPenFnDataByteArray"
    :seq     "pigpen.PigPenFnDataBag"
    :boolean "pigpen.PigPenFnBoolean"
    :fold    "pigpen.PigPenFnAlgebraic"))

(defn eval-udf
  [func ^Tuple t]
  "Evaluates a pig tuple as a clojure function. The first element of the tuple
   is any initialization code. The second element is the function to be called.
   Any remaining args are passed to the function as a collection."
  (try
    (let [args (.getAll t)]
      (func args))
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
    (ae/safe->!! ch value)))

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
                     [(ae/safe-go (ae/chan->lazy-seq c)) c])
                   [a nil])))
    (apply map vector)))

(defn ^:private create-accumulate-state
  "Creates a new accumulator state. This is a vector with two elements. The
first is the channels to pass future values to. The second is a channel
containing the singe value of the result."
  [func ^Tuple tuple]
  (let [args (.getAll tuple)]
    ;; Run init code if present
    ;; Make new lazy bags & create a result channel
    (let [[args* input-bags] (lazy-bag-args args)
          ;; Start result evaluation asynchronously, it will block on lazy bags
          result (ae/safe-go (func args*))]
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
  [func [input-bags result] ^Tuple tuple]
  (try
    (if-not result ; have we started processing this value yet?

      ;; create new channels
      (let [state (create-accumulate-state func tuple)]
        (udf-accumulate func state tuple) ;; actually push the initial values
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
  {:pre [(ae/channel? result)]}
  (doseq [b input-bags
          :when b]
    (a/close! b))
  (ae/safe-<!! result))

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

(defmethod pigpen.runtime/pre-process [:pig :native]
  [_ _]
  identity)

(defmethod pigpen.runtime/pre-process [:pig :frozen]
  [_ _]
  (fn [args]
    (mapv rt/hybrid->clojure args)))

(defmethod pigpen.runtime/post-process [:pig :native]
  [_ _]
  (fn [args]
    (apply tuple args)))

(defmethod pigpen.runtime/post-process [:pig :frozen]
  [_ _]
  (fn [args]
    (apply tuple
      (mapv pig-freeze args))))

(defmethod pigpen.runtime/post-process [:pig :frozen-with-nils]
  [_ _]
  (fn [args]
    (apply tuple
      (mapv pig-freeze-with-nils args))))

(defmethod pigpen.runtime/post-process [:pig :native-key-frozen-val]
  [_ _]
  (fn [[key value]]
    (apply tuple
      [key (pig-freeze value)])))

;; TODO lots of duplication here
(defn exec-initial
  "Special exec function for fold. Input will always be a frozen bag. Returns a single frozen value in a tuple."
  [pre seed reducef args]
  (->> args
    first
    rt/hybrid->clojure
    pre
    (reduce reducef seed)
    pig-freeze
    tuple))

(defn exec-intermed
  "Special exec function for fold. Input will always be a frozen bag. Returns a single frozen value in a tuple."
  [combinef args]
  (->> args
    first
    rt/hybrid->clojure
    (reduce combinef)
    pig-freeze
    tuple))

(defn exec-final
  "Special exec function for fold. Input will always be a frozen bag. Returns a single frozen value."
  [combinef post args]
  (->> args
    first
    rt/hybrid->clojure
    (reduce combinef)
    post
    pig-freeze))

(defn split-bag
  "Splits a single databag into multiple bags"
  [^DataBag b]
  (some->> b
    (.iterator)
    iterator-seq
    (split-at (/ (.size b) 2))
    (map (partial apply bag))))

(defn udf-algebraic
  "Evaluates an algebraic function. An algebraic function has three stages: the
initial reduce, a combiner, and a final stage."
  [foldf type ^Tuple t]
  (try
    (let [args (.getAll t)
          {:keys [pre combinef reducef post]} foldf]
      (case type
        :initial
        (exec-initial pre (combinef) reducef args)

        :intermed
        (exec-intermed combinef args)

        :final
        (exec-final combinef post args)

        ;; This is only used locally, so we split the input bag to test combinef
        ;; TODO I was wrong, this will be used on the cluster.
        ;; Need a better fix for using folds in a cogroup
        :exec
        (->> args
          (mapcat split-bag)
          (map vector)
          (map (partial exec-initial pre (combinef) reducef))
          (apply bag)
          vector
          (exec-intermed combinef)
          bag
          vector
          (exec-final combinef post))))

    (catch Throwable z (throw (PigPenException. z)))))

(defn pre-process*
  [type value]
  (case type
    :frozen (rt/hybrid->clojure value)
    :native value))

(defn get-partition
  "A hadoop custom partitioner"
  [type func key n]
  (int (func n (pre-process* (keyword type) key))))
