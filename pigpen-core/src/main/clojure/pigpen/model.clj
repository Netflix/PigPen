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

(ns pigpen.model
  (:require [schema.core :as s]))

(defn field?
  "Determines if a symbol is a valid pig identifier"
  [id]
  (boolean
    (and (symbol? id)
         (namespace id)
         (re-find #"^[a-zA-Z][a-zA-Z0-9_]*$" (namespace id))
         (re-find #"^[a-zA-Z$][a-zA-Z0-9_]*$" (name id)))))

(s/defschema Field
  (s/pred field? "field?"))

(s/defschema FieldType
  (s/enum :native :frozen :frozen-with-nils :native-key-frozen-val))

(s/defschema FieldExpr
  {:type (s/eq :field)
   :field Field})

(s/defschema CodeExpr
  {:type (s/eq :code)
   :init s/Any
   :func s/Any
   :udf (s/enum :seq :fold)
   :args [(s/either Field s/Str)]})

(s/defschema Op*
  {:id s/Symbol
   (s/optional-key :description) (s/maybe s/Str)
   (s/optional-key :opts) {s/Keyword s/Any}})

(s/defschema Op
  (merge Op*
         {:fields [Field]
          :field-type FieldType}))

(s/defschema Ancestor
  (merge Op
         {s/Keyword s/Any}))

(defmacro defop-zero [name s]
  `(do
     (s/defschema ~(symbol (str (clojure.core/name name) "$"))
       ~s)
     (s/defschema ~name
       ~s)))

(defmacro defop-one [name s]
  `(do
     (s/defschema ~(symbol (str (clojure.core/name name) "$"))
       (merge ~s
              {:ancestors [(s/one Ancestor "ancestor")]}))
     (s/defschema ~name
       (merge ~s
              {:ancestors [(s/one s/Symbol "ancestor")]}))))

(defmacro defop-many [name s]
  `(do
     (s/defschema ~(symbol (str (clojure.core/name name) "$"))
       (merge ~s
              {:ancestors [Ancestor]}))
     (s/defschema ~name
       (merge ~s
              {:ancestors [s/Symbol]}))))

(defop-zero Load
  (merge Op
         {:type (s/eq :load)
          :location s/Str
          :storage s/Keyword}))

(defop-one Store
  (merge Op*
         {:type (s/eq :store)
          :location s/Str
          :storage s/Keyword
          :args [Field]}))

(defop-zero Return
  (merge Op
         {:type (s/eq :return)
          :data [s/Any]}))

(s/defschema Projection
  {:type (s/eq :projection)
   :expr (s/either CodeExpr FieldExpr)
   :flatten s/Bool
   :alias [Field]
   (s/optional-key :types) [s/Keyword]})

(defop-one Project
  (merge Op
         {:type (s/eq :project)
          :projections [Projection]}))

(defop-one Bind
  (merge Op*
         {:type           (s/eq :bind)
          :func           s/Any
          :args           [(s/either Field s/Str)]
          :requires       [s/Symbol]
          :fields         [Field]
          :field-type-in  FieldType
          :field-type     FieldType
          :types          [s/Keyword]}))

(defop-one Sort
  (merge Op
         {:type (s/eq :sort)
          :key Field
          :comp (s/enum :asc :desc)}))

(defop-one Rank
  (merge Op
         {:type (s/eq :rank)}))

(defop-one Filter
  (merge Op
         {:type (s/eq :filter)
          :field-type (s/eq :native)
          :expr s/Any})) ;; TODO make better

(defop-one Take
  (merge Op
         {:type (s/eq :take)
          :n (s/pred (every-pred number? pos?) "positive number")}))

(defop-one Sample
  (merge Op
         {:type (s/eq :sample)
          :p (s/pred #(and (float? %) (<= 0.0 % 1.0)) "percentage")}))

(defop-one Distinct
  (merge Op
         {:type (s/eq :distinct)}))

(defop-many Concat
  (merge Op
         {:type (s/eq :concat)}))

(defop-one Reduce
  (merge Op
         {:type (s/eq :reduce)
          :arg Field}))

(defop-many Group
  (merge Op
         {:type (s/eq :group)
          :field-dispatch (s/enum :group :set)
          :keys [Field]
          :join-types [(s/enum :required :optional)]}))

(defop-many Join
  (merge Op
         {:type (s/eq :join)
          :field-dispatch (s/enum :join)
          :keys [Field]
          :join-types [(s/enum :required :optional)]}))

(defop-many NoOp
  (merge Op
         {:type (s/eq :noop)
          :args [Field]}))

(s/defschema StoreMany$
  {:type (s/eq :store-many)
   :id s/Symbol
   :ancestors [(s/either Store$ (s/recursive #'StoreMany$))]})

(s/defschema StoreMany
  {:type (s/eq :store-many)
   :id s/Symbol
   :ancestors [s/Symbol]})
