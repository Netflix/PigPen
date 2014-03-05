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
  "Contains fold operations for use with pig/fold, pig/group-by, and pig/cogroup.

See https://github.com/Netflix/PigPen/wiki/Folding-Data
"
  (:refer-clojure :exclude [vec map mapcat filter remove distinct keep take first last sort sort-by juxt count min min-key max max-key])
  (:require [pigpen.join :refer [fold-fn*]]
            [pigpen.util :refer [zipv]]))

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
  ([reducef] (fold-fn identity reducef reducef identity))
  ([combinef reducef] (fold-fn identity combinef reducef identity))
  ([combinef reducef post] (fold-fn identity combinef reducef post))
  ([pre combinef reducef post]
    (fold-fn* pre combinef reducef post)))

;; TODO interop
;; TODO add assertions that folds are folds

(defn ^:private seq-fold? [fold]
  (and
    (-> fold :combinef meta :seq)
    (-> fold :reducef meta :seq)))

(defn ^:private comp-pre
  [f {:keys [pre combinef reducef post] :as fold}]
  (assert (seq-fold? fold) (str "Operator must be used before aggregation."))
  (fold-fn (comp f pre)
           combinef
           reducef
           post))

(defn ^:private comp-fold
  [f {:keys [pre combinef reducef post]}]
  (fold-fn pre
           (comp f combinef)
           (comp f reducef)
           post))

(defn ^:private comp-fold-new
  [{:keys [pre] :as fold} {:keys [combinef reducef post]}]
  (fold-fn pre
           combinef
           reducef
           post))

(defn ^:private comp-post
  [f {:keys [pre combinef reducef post]}]
  (fold-fn pre
           combinef
           reducef
           (comp f post)))

(defn vec
  "Returns all values as a vector. This is the default fold operation if none
other is specified.

  Example:
    (fold/vec)

    (->> (fold/vec)
      (fold/take 5))
"
  []
  (fold-fn ^:seq (fn
                   ([] [])
                   ([l r] (vec (concat l r))))
           ^:seq (fn [acc val] (conj acc val))))

(defn preprocess
  "Takes a a clojure seq function, like map or filter, and returns a fold
preprocess function. The function must take two params: a function and a seq.

  Example:

    (def map (preprocess clojure.core/map))

    (pig/fold (map :foo))
"
  [f']
  (fn
    ([f]
      (comp-pre (partial f' f) (vec)))
    ([f fold]
      (comp-pre (partial f' f) fold))))

(defmacro ^:private def-preprocess
  ""
  [name fn]
  `(def ~(with-meta name {:arglists ''([f] [f fold])})
     ~(str "Pre-processes data for a fold operation. Same as " fn ".")
     (preprocess ~fn)))

(def-preprocess map clojure.core/map)
(def-preprocess mapcat clojure.core/mapcat)
(def-preprocess filter clojure.core/filter)
(def-preprocess remove clojure.core/remove)
(def-preprocess keep clojure.core/keep)

(defn distinct
  "Returns the distinct set of values.

  Example:
    (fold/distinct)

    (->> (fold/map :foo)
         (fold/keep identity)
         (fold/distinct))
"
  ([]
    (fold-fn clojure.set/union conj))
  ([fold]
    (comp-fold-new fold (distinct))))

(defn take
  "Returns a sequence of the first n items in coll. This is a post-reduce
operation, meaning that it can only be applied after a fold operation that
produces a sequence.

  Example:

    (->>
      (fold/sort)
      (fold/take 40))
"
  ([n] (take n (vec)))
  ([n fold]
    (comp-fold (partial clojure.core/take n) fold)))

(defn first
  "Returns the first output value. This is a post-reduce operation, meaning that
it can only be applied after a fold operation that produces a sequence.

  Example:
    (fold/first)

    (->> (fold/map :foo)
         (fold/sort)
         (fold/first))

  See also: pigpen.fold/last, pigpen.fold/min, pigpen.fold/max
"
  ([] (first (vec)))
  ([fold]
    (->> fold
      (comp-fold (partial clojure.core/take 1))
      (comp-post clojure.core/first))))

(defn last
  "Returns the last output value. This is a post-reduce operation, meaning that
it can only be applied after a fold operation that produces a sequence.

  Example:
    (fold/last)

    (->> (fold/map :foo)
         (fold/sort)
         (fold/last))

  See also: pigpen.fold/first, pigpen.fold/min, pigpen.fold/max
"
  ([] (last (vec)))
  ([fold]
    (->> fold
      (comp-fold reverse)
      (comp-fold (partial clojure.core/take 1))
      (comp-post clojure.core/last))))

(defn sort
  "Sorts the data. This sorts the data after every element, so it's best to use
with take, which also limits the data after every value. If a comparator is not
specified, clojure.core/compare is used.

  Example:

    (fold/sort)

    (->>
      (fold/sort)
      (fold/take 40))

    (->>
      (fold/sort >)
      (fold/take 40))

  See also: pigpen.fold/sort-by, pigpen.fold/top
"
  ([] (sort compare (vec)))
  ([fold] (sort compare fold))
  ([c fold]
    (comp-fold (partial clojure.core/sort c) fold)))

(defn sort-by
  "Sorts the data by (keyfn value). This sorts the data after every element, so
it's best to use with take, which also limits the data after every value. If a
comparator is not specified, clojure.core/compare is used.

  Example:

    (fold/sort-by :foo)

    (->> (vec)
      (fold/sort-by :foo)
      (fold/take 40))

    (->> (vec)
      (fold/sort-by :foo >)
      (fold/take 40))

  See also: pigpen.fold/sort, pigpen.fold/top-by
"
  ([keyfn] (sort-by keyfn compare (vec)))
  ([keyfn fold] (sort-by keyfn compare fold))
  ([keyfn c fold]
    (comp-fold (partial clojure.core/sort-by keyfn c) fold)))

(defn juxt
  "Applies multiple fold fns to the same data. Produces a vector of results.

  Example:
    (fold/juxt (fold/count) (fold/sum) (fold/avg))
"
 [& folds]
 (fold-fn
   ; pre
   (fn [vals]
     (for [v vals]
       (zipv [{:keys [pre]} folds]
         (pre [v]))))
   ; combine
   (fn
     ([]
       (zipv [{:keys [combinef]} folds]
         (combinef)))
     ([l r]
       (zipv [{:keys [combinef]} folds
              l' l
              r' r]
         (combinef l' r'))))
   ; reduce
   (fn [acc val]
     (zipv [{:keys [reducef]} folds
            a' acc
            v' val]
       (reduce reducef a' v')))
   ; post
   (fn [vals]
     (zipv [{:keys [post]} folds
            v' vals]
       (post v')))))

(defn count
  "Counts the values, including nils. Optionally takes another fold operation
to compose.

  Example:
    (fold/count)

    (->> (fold/keep identity) (fold/count)) ; count non-nils
    (->> (fold/filter #(< 0 %)) (fold/count)) ; count positive numbers

    (->>
      (fold/map :foo)
      (fold/keep identity)
      (fold/count))

  See also: pigpen.fold/sum, pigpen.fold/avg
"
  ([]
    (fold-fn + (fn [acc _] (inc acc))))
  ([fold]
    (comp-fold-new fold (count))))

(defn sum
  "Sums the values. All values must be numeric. Optionally takes another
fold operation to compose.

  Example:
    (fold/sum)

    (->> (fold/map :foo) (fold/sum)) ; sum the foo's
    (->> (fold/keep identity) (fold/sum)) ; sum non-nils
    (->> (fold/filter #(< 0 %)) (fold/sum)) ; sum positive numbers

    (->>
      (fold/map :foo)
      (fold/keep identity)
      (fold/sum))

  See also: pigpen.fold/count, pigpen.fold/avg
"
  ([]
    (fold-fn +))
  ([fold]
    (comp-fold-new fold (sum))))

(defn avg
  "Average the values. All values must be numeric. Optionally takes another
fold operation to compose.

  Example:
    (fold/avg)

    (->> (fold/map :foo) (fold/avg)) ; average the foo's
    (->> (fold/keep identity) (fold/avg)) ; avg non-nils
    (->> (fold/filter #(< 0 %)) (fold/avg)) ; avg positive numbers

    (->>
      (fold/map :foo)
      (fold/keep identity)
      (fold/avg))

  See also: pigpen.fold/count, pigpen.fold/sum
"
  ([]
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
  ([fold]
    (comp-fold-new fold (avg))))

(defn top
  "Returns the top n items in the collection. If a comparator is not specified,
clojure.core/compare is used.

  Example:
    (fold/top 40)
    (fold/top > 40)

  See also: pigpen.fold/top-by
"
  ([n] (top compare n))
  ([comp n]
    (->> (vec)
      (sort comp)
      (take n))))

(defn top-by
  "Returns the top n items in the collection based on (keyfn value). If a
comparator is not specified, clojure.core/compare is used.

  Example:
    (fold/top-by :foo 40)
    (fold/top-by :foo > 40)

  See also: pigpen.fold/top
"
  ([keyfn n] (top-by keyfn compare n))
  ([keyfn comp n]
    (->> (vec)
      (sort-by keyfn comp)
      (take n))))

(defn ^:private min*
  "Returns the best item from the collection. Optionally specify a comparator."
  [comp fold]
  {:pre [(instance? java.util.Comparator comp)]}
  (comp-fold-new fold
                 (fold-fn (fn
                            ([] ::nil)
                            ([l r]
                              (cond
                                (= ::nil l) r
                                (= ::nil r) l
                                (< (comp l r) 0) l
                                :else r))))))

(defn ^:private compare-by
  ([keyfn] (compare-by keyfn compare))
  ([keyfn comp] #(comp (keyfn %1) (keyfn %2))))

(defn min
  "Return the minimum (first) value of the collection. If a comparator is not
specified, clojure.core/compare is used. Optionally takes another fold
operation to compose.

  Example:
    (fold/min)
    (fold/min >)

    (->>
      (fold/map :foo)
      (fold/min >))

  See also: pigpen.fold/min-key, pigpen.fold/max, pigpen.fold/top
"
  {:arglists '([] [fold] [comp] [comp fold])}
  ([] (min* compare (vec)))
  ([fold]
    (if (instance? java.util.Comparator fold)
      (min* fold (vec))
      (min* compare fold)))
  ([comp fold] (min* comp fold)))

(defn min-key
  "Return the minimum (first) value of the collection based on (keyfn value).
If a comparator is not specified, clojure.core/compare is used. Optionally takes
another fold operation to compose.

  Example:
    (fold/min-key :foo)
    (fold/min-key :foo >)

  See also: pigpen.fold/min, pigpen.fold/max-key, pigpen.fold/top-by
"
  {:arglists '([keyfn] [keyfn fold] [keyfn comp] [keyfn comp fold])}
  ([keyfn] (min-key keyfn compare (vec)))
  ([keyfn fold]
    (if (instance? java.util.Comparator fold)
      (min-key keyfn fold (vec))
      (min-key keyfn compare fold)))
  ([keyfn comp fold] (min* (compare-by keyfn comp) fold)))

(defn max
  "Return the maximum (last) value of the collection. If a comparator is not
specified, clojure.core/compare is used. Optionally takes another fold
operation to compose.

  Example:
    (fold/max)
    (fold/max >)

    (->>
      (fold/map :foo)
      (fold/max >))

  See also: pigpen.fold/max-key, pigpen.fold/min, pigpen.fold/top
"
  {:arglists '([] [fold] [comp] [comp fold])}
  ([] (min* (clojure.core/comp - compare) (vec)))
  ([fold]
    (if (instance? java.util.Comparator fold)
      (min* (clojure.core/comp - fold) (vec))
      (min* (clojure.core/comp - compare) fold)))
  ([comp fold] (min* (clojure.core/comp - comp) fold)))

(defn max-key
  "Return the maximum (last) value of the collection based on (keyfn value).
If a comparator is not specified, clojure.core/compare is used. Optionally takes
another fold operation to compose.

  Example:
    (fold/max-key :foo)
    (fold/max-key :foo >)

  See also: pigpen.fold/max, pigpen.fold/min-key, pigpen.fold/top-by
"
  {:arglists '([keyfn] [keyfn fold] [keyfn comp] [keyfn comp fold])}
  ([keyfn] (max-key keyfn compare (vec)))
  ([keyfn fold]
    (if (instance? java.util.Comparator fold)
      (max-key keyfn fold (vec))
      (max-key keyfn compare fold)))
  ([keyfn comp fold] (min* (clojure.core/comp - (compare-by keyfn comp)) fold)))
