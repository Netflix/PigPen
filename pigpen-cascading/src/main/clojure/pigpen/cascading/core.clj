(ns pigpen.cascading.core
  "Contains the operators for PigPen."
  (:refer-clojure :exclude [load-string constantly map mapcat map-indexed sort sort-by filter remove distinct concat take group-by into reduce])
  (:require [pigpen.cascading]
            [pigpen.raw :as raw]
            [pigpen.map]
            [pigpen.filter]
            [pigpen.set]
            [pigpen.join]))

;; ********** Flow **********
(intern *ns* (with-meta 'generate-flow (meta #'pigpen.cascading/generate-flow)) @#'pigpen.cascading/generate-flow)

;; ********** Custom loaders **********
(intern *ns* (with-meta 'load-tap (meta #'pigpen.cascading/load-tap)) @#'pigpen.cascading/load-tap)

;; ********** Custom store functions **********
(intern *ns* (with-meta 'store-tap (meta #'pigpen.cascading/store-tap)) @#'pigpen.cascading/store-tap)

