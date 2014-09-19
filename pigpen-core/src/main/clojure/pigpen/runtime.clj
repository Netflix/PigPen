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

(defn eval-string
  "Reads code from a string & evaluates it"
  [f]
  (when (not-empty f)
    (try
      (eval (read-string f))
      (catch Throwable z
        (throw (RuntimeException. (str "Exception evaluating: " f) z))))))
