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

(ns pigpen.code
  "Contains functions that assist in handling user code in operations like map
or reduce."
  (:require [pigpen.pig :as pig]
            [pigpen.raw :as raw]
            [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.utils :as nippy-util])
  (:import [org.apache.pig.data DataBag]
           [java.lang.reflect Method]))

(set! *warn-on-reflection* true)

(defn arity
  "Returns the arities of the invoke methods for f.
   Also returns the minimum varargs arity"
  [f]
  {:pre [f]}
  (let [methods (-> f class .getDeclaredMethods)
        fixed (->> methods
                (filter (fn [^Method m] (= "invoke" (.getName m))))
                (map (fn [^Method m] (-> m .getParameterTypes alength)))
                set)
        varargs (->> methods
                  (filter (fn [^Method m] (= "doInvoke" (.getName m))))
                  (map (fn [^Method m] (-> m .getParameterTypes alength dec)))
                  first)]
    [fixed varargs]))

(defn format-arity [fixed varargs]
  (let [symbols (->> (range 97 123)
                  (map (comp symbol str char)))
        fixed (for [a fixed]
                (vec (take a symbols)))
        varargs (if varargs
                  [(vec
                     (concat
                       (vec (take (- varargs 1) symbols))
                       '[& more]))])]
    (concat fixed varargs)))

(defn assert-arity* [f' n]
  {:pre [f' (integer? n) (<= 0 n)]}
  (let [[fixed varargs] (arity f')]
    (assert
      (or (fixed n) (if varargs (<= varargs n)))
      (str "Expecting arity: " n " Found arities: "
           (pr-str (format-arity fixed varargs))))))

(defn assert-arity [f n]
  {:pre [f]}
  (let [f' (eval f)
        [fixed varargs] (arity f')]
    (assert-arity* f' n)))

;; TODO add an option to make the default include/exclude configurable

(defn ^:private make-binding [k v]
  (cond
    (fn? v) nil
    (:pig (meta v)) nil
    (:local (meta v)) nil
    (:local (meta k)) nil
    (not (nippy-util/readable? v)) nil
    ;; TODO if something is freezable, but not EDN friendly, maybe use a base64 string?
    ;(nippy/freezeable? v) [k `(thaw (decode ~(encode (freeze v))))]
    :else [k `(quote ~v)]))

(defn resource-exists
  "Converts a ns to a resource with the specified ext and checks for existence"
  [ns ext]
  (as-> ns %
    (clojure.string/replace % "." "/")
    (clojure.string/replace % "-" "_")
    (str % ext)
    (clojure.java.io/resource %)))

(defn ns-exists
  "Returns the ns if it exists as a resource"
  [ns]
  (when (and ns (or (resource-exists ns ".clj")
                    (resource-exists ns "__init.class")))
    ns))

(defn build-requires [nss]
  (->> nss
    (filter ns-exists)
    (cons 'pigpen.pig)
    (distinct)
    (map (fn [r] `'[~r]))
    (cons 'clojure.core/require)))

(defn trap-locals [keys values f]
  (let [args (vec (mapcat make-binding keys values))]
    (if (not-empty args)
      `(let ~args ~f)
      f)))

(defn trap-ns [ns f]
  (if (ns-exists ns)
    `(pig/with-ns ~ns ~f)
    f))

(defn trap* [keys values ns f]
  (->> f
    (trap-locals keys values)
    (trap-ns ns)))

(defmacro trap
  "Returns a form that, when evaluated, will reconsitiute f in namespace ns, in
the presence of any local bindings"
  ([f] `(trap '~(ns-name *ns*) ~f))
  ([ns f]
    (let [keys# (vec (keys &env))]
      `(trap* '~keys# ~keys# ~ns '~f))))

(defn trap-values
  "Takes a sequence of options , converts them into a map (if not already), and
optionally traps specific values. The parameter quotable determines which ones
should be quoted and trapped."
  [quotable values]
  (let [values' (cond
                  (map? values) values
                  (sequential? values) (partition 2 values)
                  :else (throw (IllegalArgumentException. "Unknown values")))]
    (->> values'
      (map (fn [[k v]]
             (let [k (keyword k)]
               [k (if (quotable k) `(trap ~v) v)])))
      (clojure.core/into {}))))
