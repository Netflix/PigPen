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

(ns pigpen.runtime
  "Functions for evaluating user code at runtime")

(defmacro with-ns
  "Evaluates f within ns. Calls (require 'ns) first."
  [ns f]
  `(do
     (require '~ns)
     (binding [*ns* (find-ns '~ns)]
       (eval '~f))))

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

(defn process->bind
  "Wraps the output of pre- and post-process with a vector."
  [f]
  (comp vector f))

(defn sentinel-nil
  "Coerces nils into a sentinel value. Useful for nil handling in outer joins."
  [value]
  (if (nil? value)
    ::nil
    value))

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
    (->> args (mapcat (juxt type str)) (clojure.string/join "\t"))
    (catch Exception z (str "Error getting value: " z))))

(defn eval-string
  "Reads code from a string & evaluates it"
  [f]
  (when (not-empty f)
    (try
      (eval (read-string f))
      (catch Throwable z
        (throw (RuntimeException. (str "Exception evaluating: " f) z))))))

(defmulti pre-process
  "Optionally deserializes incoming data. Should return a fn that takes a single
'args' param and returns a seq of processed args. 'platform' is what will be
running the code (:pig, :cascading, etc). 'serialization-type' defines which of
the fields to serialize. It will be one of:

  :native - everything should be native values. You may need to coerce host
            types into clojure types here.
  :frozen - deserialize everything
"
  (fn [platform serialization-type]
    [platform serialization-type]))

(defmethod pre-process :default [_ _] identity)

(defmulti post-process
  "Optionally serializes outgoing data. Should return a fn that takes a single
'args' param and returns a seq of processed args. 'platform' is what will be
running the code (:pig, :cascading, etc). 'serialization-type' defines which of
the fields to serialize. It will be one of:

  :native - don't serialize
  :frozen - serialize everything
  :frozen-with-nils - serialize everything except nils
  :native-key-frozen-val - expect a tuple, freeze only the second value
"
  (fn [platform serialization-type]
    [platform serialization-type]))

(defmethod post-process :default [_ _] identity)

(defn exec
  "Applies the composition of fs, flattening intermediate results. Each f must
produce a seq-able output that is flattened as input to the next command."
  [fs]
  (fn [args]
    (reduce (fn [vs f] (mapcat f vs)) [args] fs)))
