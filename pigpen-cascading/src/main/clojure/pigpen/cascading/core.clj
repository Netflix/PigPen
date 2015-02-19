(ns pigpen.cascading.core
  "Contains the operators for generating cascading flows in PigPen"
  (:require [pigpen.cascading]))

;; ********** Flow **********
(intern *ns* (with-meta 'generate-flow (meta #'pigpen.cascading/generate-flow)) @#'pigpen.cascading/generate-flow)

;; ********** Customer loaders **********
(intern *ns* (with-meta 'load-tap (meta #'pigpen.cascading/load-tap)) @#'pigpen.cascading/load-tap)

