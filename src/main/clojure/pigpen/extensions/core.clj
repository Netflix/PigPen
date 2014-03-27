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

(ns pigpen.extensions.core
  (:require [clojure.pprint])
  (:import [java.io StringWriter]))

(set! *warn-on-reflection* true)

(defn pp-str
 "Pretty prints to a string"
 [object]
 (let [writer (StringWriter.)]
   (clojure.pprint/pprint object writer)
   (.toString writer)))

(defmacro zip
  "Syntax of for, semantics of map.

  Example:
		=> (zip [x [1 2 3]
		         y [:a :b :c]
		         z [\"foo\" \"bar\" \"baz\"]]
		     [x y z])
		([1 :a \"foo\"] [2 :b \"bar\"] [3 :c \"baz\"])
"
  [bindings & body]
  (let [bindings# (partition 2 bindings)
        vars# (mapv first bindings#)
        vals# (mapv second bindings#)]
    `(map (fn ~vars# ~@body) ~@vals#)))

(defmacro zipv
  "Syntax of for, semantics of mapv.

  Example:
		=> (zipv [x [1 2 3]
		          y [:a :b :c]
		          z [\"foo\" \"bar\" \"baz\"]]
		     [x y z])
		[[1 :a \"foo\"] [2 :b \"bar\"] [3 :c \"baz\"]]
"
  [bindings & body]
  (let [bindings# (partition 2 bindings)
        vars# (mapv first bindings#)
        vals# (mapv second bindings#)]
    `(mapv (fn ~vars# ~@body) ~@vals#)))

(defmacro forcat
  "Returns the result of applying concat to a for expression."
  [seq-exprs body-expr]
  `(apply concat
     (for ~seq-exprs ~body-expr)))
