;;
;;
;;  Copyright 2013-2015 Netflix, Inc.
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

(ns pigpen.core.fn
  "*** ALPHA - Subject to change ***

  Function versions of the core pigpen macros. These are useful if you want to
generate more dynamic scripts, where functions are passed as arguments. This
also means that the functions passed must either be quoted manually or with
pigpen.core.fn/trap. See pigpen.core.fn/map* for example.

  Note: You most likely don't want this namespace. Unless you are doing advanced
things, stick to pigpen.core
"
  (:require [pigpen.code]
            [pigpen.io]
            [pigpen.map]
            [pigpen.filter]
            [pigpen.set]
            [pigpen.join]))

(set! *warn-on-reflection* true)

(intern *ns* (with-meta 'trap (meta #'pigpen.code/trap)) @#'pigpen.code/trap)

;; ********** IO **********

(intern *ns* (with-meta 'load-string* (meta #'pigpen.io/load-string*)) @#'pigpen.io/load-string*)
(intern *ns* (with-meta 'store-string* (meta #'pigpen.io/store-string*)) @#'pigpen.io/store-string*)

;; ********** Map **********

(intern *ns* (with-meta 'map* (meta #'pigpen.map/map*)) @#'pigpen.map/map*)
(intern *ns* (with-meta 'mapcat* (meta #'pigpen.map/mapcat*)) @#'pigpen.map/mapcat*)
(intern *ns* (with-meta 'map-indexed* (meta #'pigpen.map/map-indexed*)) @#'pigpen.map/map-indexed*)
(intern *ns* (with-meta 'sort* (meta #'pigpen.map/sort*)) @#'pigpen.map/sort*)

;; ********** Filter **********

(intern *ns* (with-meta 'filter* (meta #'pigpen.filter/filter*)) @#'pigpen.filter/filter*)

;; ********** Set **********

(intern *ns* (with-meta 'set-op* (meta #'pigpen.set/set-op*)) @#'pigpen.set/set-op*)

;; ********** Join **********

(intern *ns* (with-meta 'group* (meta #'pigpen.join/group*)) @#'pigpen.join/group*)
(intern *ns* (with-meta 'reduce* (meta #'pigpen.join/reduce*)) @#'pigpen.join/reduce*)
(intern *ns* (with-meta 'fold* (meta #'pigpen.join/fold*)) @#'pigpen.join/fold*)
(intern *ns* (with-meta 'join* (meta #'pigpen.join/join*)) @#'pigpen.join/join*)
