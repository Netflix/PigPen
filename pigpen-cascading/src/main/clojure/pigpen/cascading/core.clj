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

