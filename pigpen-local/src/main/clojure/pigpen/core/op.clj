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

(ns pigpen.core.op
  (:require [pigpen.raw]))

(intern *ns* (with-meta 'noop$ (meta #'pigpen.raw/noop$)) @#'pigpen.raw/noop$)

;; ********** IO **********

(intern *ns* (with-meta 'load$ (meta #'pigpen.raw/load$)) @#'pigpen.raw/load$)
(intern *ns* (with-meta 'store$ (meta #'pigpen.raw/store$)) @#'pigpen.raw/store$)
(intern *ns* (with-meta 'store-many$ (meta #'pigpen.raw/store-many$)) @#'pigpen.raw/store-many$)
(intern *ns* (with-meta 'return$ (meta #'pigpen.raw/return$)) @#'pigpen.raw/return$)

;; ********** Map **********

(intern *ns* (with-meta 'code$ (meta #'pigpen.raw/code$)) @#'pigpen.raw/code$)
(intern *ns* (with-meta 'projection-field$ (meta #'pigpen.raw/projection-field$)) @#'pigpen.raw/projection-field$)
(intern *ns* (with-meta 'projection-func$ (meta #'pigpen.raw/projection-func$)) @#'pigpen.raw/projection-func$)
(intern *ns* (with-meta 'project$ (meta #'pigpen.raw/project$)) @#'pigpen.raw/project$)
(intern *ns* (with-meta 'bind$ (meta #'pigpen.raw/bind$)) @#'pigpen.raw/bind$)
(intern *ns* (with-meta 'sort$ (meta #'pigpen.raw/sort$)) @#'pigpen.raw/sort$)
(intern *ns* (with-meta 'rank$ (meta #'pigpen.raw/rank$)) @#'pigpen.raw/rank$)

;; ********** Filter **********

(intern *ns* (with-meta 'take$ (meta #'pigpen.raw/take$)) @#'pigpen.raw/take$)
(intern *ns* (with-meta 'sample$ (meta #'pigpen.raw/sample$)) @#'pigpen.raw/sample$)

;; ********** Set **********

(intern *ns* (with-meta 'distinct$ (meta #'pigpen.raw/distinct$)) @#'pigpen.raw/distinct$)
(intern *ns* (with-meta 'concat$ (meta #'pigpen.raw/concat$)) @#'pigpen.raw/concat$)

;; ********** Join **********

(intern *ns* (with-meta 'reduce$ (meta #'pigpen.raw/reduce$)) @#'pigpen.raw/reduce$)
(intern *ns* (with-meta 'group$ (meta #'pigpen.raw/group$)) @#'pigpen.raw/group$)
(intern *ns* (with-meta 'join$ (meta #'pigpen.raw/join$)) @#'pigpen.raw/join$)
