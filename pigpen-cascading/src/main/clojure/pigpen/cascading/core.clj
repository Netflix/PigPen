(ns pigpen.cascading.core
  "Contains the operators for PigPen."
  (:refer-clojure :exclude [load-string constantly map mapcat map-indexed sort sort-by filter remove distinct concat take group-by into reduce])
  (:require [pigpen.raw :as raw]
            [pigpen.map]
            [pigpen.filter]
            [pigpen.set]
            [pigpen.join]))

;; ********** Map **********

(intern *ns* (with-meta 'map (meta #'pigpen.map/map)) @#'pigpen.map/map)
(intern *ns* (with-meta 'mapcat (meta #'pigpen.map/mapcat)) @#'pigpen.map/mapcat)
(intern *ns* (with-meta 'map-indexed (meta #'pigpen.map/map-indexed)) @#'pigpen.map/map-indexed)
(intern *ns* (with-meta 'sort (meta #'pigpen.map/sort)) @#'pigpen.map/sort)
(intern *ns* (with-meta 'sort-by (meta #'pigpen.map/sort-by)) @#'pigpen.map/sort-by)

;; ********** Filter **********

(intern *ns* (with-meta 'filter (meta #'pigpen.filter/filter)) @#'pigpen.filter/filter)
(intern *ns* (with-meta 'remove (meta #'pigpen.filter/remove)) @#'pigpen.filter/remove)
(intern *ns* (with-meta 'take (meta #'pigpen.filter/take)) @#'pigpen.filter/take)
(intern *ns* (with-meta 'sample (meta #'pigpen.filter/sample)) @#'pigpen.filter/sample)

;; ********** Set **********

(intern *ns* (with-meta 'distinct (meta #'pigpen.set/distinct)) @#'pigpen.set/distinct)
(intern *ns* (with-meta 'union (meta #'pigpen.set/union)) @#'pigpen.set/union)
(intern *ns* (with-meta 'concat (meta #'pigpen.set/concat)) @#'pigpen.set/concat)
(intern *ns* (with-meta 'union-multiset (meta #'pigpen.set/union-multiset)) @#'pigpen.set/union-multiset)
(intern *ns* (with-meta 'intersection (meta #'pigpen.set/intersection)) @#'pigpen.set/intersection)
(intern *ns* (with-meta 'intersection-multiset (meta #'pigpen.set/intersection-multiset)) @#'pigpen.set/intersection-multiset)
(intern *ns* (with-meta 'difference (meta #'pigpen.set/difference)) @#'pigpen.set/difference)
(intern *ns* (with-meta 'difference-multiset (meta #'pigpen.set/difference-multiset)) @#'pigpen.set/difference-multiset)

;; ********** Join **********

(intern *ns* (with-meta 'group-by (meta #'pigpen.join/group-by)) @#'pigpen.join/group-by)
(intern *ns* (with-meta 'into (meta #'pigpen.join/into)) @#'pigpen.join/into)
(intern *ns* (with-meta 'reduce (meta #'pigpen.join/reduce)) @#'pigpen.join/reduce)
(intern *ns* (with-meta 'cogroup (meta #'pigpen.join/cogroup)) @#'pigpen.join/cogroup)
(intern *ns* (with-meta 'join (meta #'pigpen.join/join)) @#'pigpen.join/join)
(intern *ns* (with-meta 'fold (meta #'pigpen.join/fold)) @#'pigpen.join/fold)
(intern *ns* (with-meta 'filter-by (meta #'pigpen.join/filter-by)) @#'pigpen.join/filter-by)
(intern *ns* (with-meta 'remove-by (meta #'pigpen.join/remove-by)) @#'pigpen.join/remove-by)

