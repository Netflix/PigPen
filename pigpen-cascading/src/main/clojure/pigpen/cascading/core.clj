(ns pigpen.cascading.core
  "Contains the operators for PigPen."
  (:refer-clojure :exclude [load-string constantly map mapcat map-indexed sort sort-by filter remove distinct concat take group-by into reduce])
  (:require [pigpen.cascading]
            [pigpen.raw :as raw]
            [pigpen.map]
            [pigpen.filter]
            [pigpen.set]
            [pigpen.join]))

;; ********** IO **********

(intern *ns* (with-meta 'load-tsv (meta #'pigpen.cascading/load-tsv)) @#'pigpen.cascading/load-tsv)
(intern *ns* (with-meta 'load-clj (meta #'pigpen.cascading/load-clj)) @#'pigpen.cascading/load-clj)

;; ********** Flow **********
(intern *ns* (with-meta 'generate-flow (meta #'pigpen.cascading/generate-flow)) @#'pigpen.cascading/generate-flow)

;; ********** Script **********

; TODO: there should be a better name for this. Script is very pig-specific.
(defn script
  "Combines multiple store commands into a single script. This is not required
if you have a single output.

  Example:

    (pig/script
      (pig/store-tsv \"foo.tsv\" foo)
      (pig/store-clj \"bar.clj\" bar))

  Note: When run locally, this will merge the results of any source relations.
"
  {:arglists '([outputs+])
   :added "0.1.0"}
  [& outputs]
  (raw/script$ outputs))
