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

(ns pigpen.fold
  (:refer-clojure :exclude [seq map mapcat filter take first last sort sort-by juxt count min min-key max max-key])
  (:require [pigpen.join :refer [fold-fn*]]))

(defn fold-fn
  "Creates a pre-defined fold operation. Can be used with cogroup and group-by
to aggregate large groupings in parallel. See pigpen.core/fold for usage of
reducef and combinef.

  Example:

    (def count
      (pig/fold-fn + (fn [acc _] (inc acc))))

    (def sum
      (pig/fold-fn +))

    (defn sum-by [f]
      (pig/fold-fn + (fn [acc value] (+ acc (f value)))))
"
  ([reducef] (fold-fn reducef reducef identity))
  ([combinef reducef] (fold-fn combinef reducef identity))
  ([combinef reducef finalf]
    (fold-fn* combinef reducef finalf)))

;; TODO interop

(defn seq
  "Returns the values as a sequence."
  []
  (fold-fn (fn
             ([] [])
             ([l r] (concat l r)))
           (fn [acc val] (conj acc val))))

(defn map
  "Applies f to each input value"
  {:arglists '([f fold])}
  [f {:keys [combinef reducef finalf]}]
  (fold-fn combinef
           (fn [acc val]
             (reducef acc (f val)))
           finalf))

(defn mapcat
  "Applies f to each input value. f must return a seq which is flattened."
  {:arglists '([f fold])}
  [f {:keys [combinef reducef finalf]}]
  (fold-fn combinef
           (fn [acc val]
             (reduce reducef acc (f val)))
           finalf))

(defn filter
  "Applies the filter fn f to each input value"
  {:arglists '([f fold])}
  [f {:keys [combinef reducef finalf]}]
  (fold-fn combinef
           (fn [acc val]
             (if (f val)
               (reducef acc val)
               acc))
           finalf))

(defn take
  "Returns a sequence of the first n items in coll."
  {:arglists '([n fold])}
  [n {:keys [combinef reducef finalf]}]
  (fold-fn (comp (partial clojure.core/take n) combinef)
           (comp (partial clojure.core/take n) reducef)
           finalf))

(defn first
  "Returns the first output value."
  {:arglists '([fold])}
  [{:keys [combinef reducef finalf]}]
  (fold-fn (comp (partial clojure.core/take 1) combinef)
           (comp (partial clojure.core/take 1) reducef)
           (comp clojure.core/first finalf)))

(defn last
  "Returns the last output value."
  {:arglists '([fold])}
  [{:keys [combinef reducef finalf]}]
  (fold-fn (comp (partial clojure.core/take 1) reverse combinef)
           (comp (partial clojure.core/take 1) reverse reducef)
           (comp clojure.core/last finalf)))

(defn sort
  "Sorts the data. This sorts the data after every element, so it's best to use with take or drop, which also limit the data after every value."
  {:arglists '([fold] [comp fold])}
  ([fold] (sort compare fold))
  ([c {:keys [combinef reducef finalf]}]
    (fold-fn (comp (partial clojure.core/sort c) combinef)
             (comp (partial clojure.core/sort c) reducef)
             finalf)))

(defn sort-by
  "Sorts the data by f. This sorts the data after every element, so it's best to use with take or drop, which also limit the data after every value."
  {:arglists '([f fold] [f comp fold])}
  ([f fold] (sort-by f compare fold))
  ([f c {:keys [combinef reducef finalf]}]
    (fold-fn (comp (partial clojure.core/sort-by f c) combinef)
             (comp (partial clojure.core/sort-by f c) reducef)
             finalf)))

(defn juxt
  "Applies multiple fold fns to the same data."
  [& folds]
  (fold-fn (fn
             ([] (mapv #((:combinef %)) folds))
             ([l r] (mapv #((:combinef %1) %2 %3) folds l r)))
           (fn [acc val] (mapv #((:reducef %1) %2 val) folds acc))
           (fn [vals] (mapv #((:finalf %1) %2) folds vals))))

(defn count
  "Counts the values."
  []
  (fold-fn + (fn [acc _] (inc acc))))

(defn count-if
  "Counts the values for which (f value) is true."
  [f]
  (filter f (count)))

(defn sum
  "Sums the values. All values must be numeric."
  []
  (fold-fn +))

(defn sum-by
  "Sum the result of calling (f value) on each value."
  [f]
  (map f (sum)))

(defn sum-if
  "Sum the values for which (f value) is true."
  [f]
  (filter f (sum)))

(defn avg
  "Average the values. All values must be numeric."
  []
  (fold-fn (fn
             ([] nil)
             ([[s0 c0] [s1 c1]]
               [(+ s0 s1) (+ c0 c1)]))
           (fn [[s c :as acc] val]
             (if acc
               [(+ s val) (inc c)]
               [val 1]))
           (fn [[s c]]
             (/ s c))))

(defn avg-by
  "Average the result of calling (f value) on each value."
  [f]
  (map f (avg)))

(defn avg-if
  "Average the values for which (f value) is true."
  [f]
  (filter f (avg)))

(defn top
  "Returns the top n items in the collection. Optionally specify a comparator."
  ([n] (top compare n))
  ([comp n]
    (->> (seq)
      (sort comp)
      (take n))))

(defn top-by
  "Returns the top n items in the collection based on (keyfn value). Optionally specify a comparator."
  ([keyfn n] (top-by keyfn compare n))
  ([keyfn comp n]
    (->> (seq)
      (sort-by keyfn comp)
      (take n))))

(defn best
  "Returns the best item from the collection. Optionally specify a comparator."
  ([] (best compare))
  ([comp]
    (fold-fn (fn
               ([] ::nil)
               ([l r]
                 (if (= ::nil l) r
                   (if (= ::nil r) l
                     (if (< (comp l r) 0) l r))))))))

(defn min
  "Return the minimum value of the collection."
  []
  (best))

(defn min-key
  "Return the minimum value of the collection based on (keyfn value)."
  [keyfn]
  (best #(compare (keyfn %1) (keyfn %2))))

(defn max
  "Return the maximum value of the collection."
  []
  (best #(- (compare %1 %2))))

(defn max-key
  "Return the maximum value of the collection based on (keyfn value)."
  [keyfn]
  (best #(- (compare (keyfn %1) (keyfn %2)))))
