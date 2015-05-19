;;
;;
;;  Copyright 2015 Netflix, Inc.
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

(ns pigpen.cascading.core
  (:import (cascading.flow FlowDef FlowConnector)
           (cascading.operation Identity)
           (cascading.operation.filter Limit Sample)
           (cascading.pipe Pipe Each Every Merge GroupBy CoGroup)
           (cascading.pipe.assembly Unique Rename AggregateBy)
           (cascading.pipe.joiner BufferJoin MixedJoin)
           (cascading.scheme.hadoop TextLine)
           (cascading.property AppProps)
           (cascading.tap Tap)
           (cascading.tap.hadoop Hfs)
           (cascading.tuple Fields)
           (cascading.util NullNotEquivalentComparator)
           (pigpen.cascading PigPenFunction PigPenAggregateBy
                             ReduceBuffer GroupBuffer
                             RankBuffer InduceSentinelNils))
  (:require [pigpen.raw :as raw]
            [schema.core :as s]
            [pigpen.model :as m]
            [pigpen.extensions.core :refer [zip]]))

(set! *warn-on-reflection* true)

(AppProps/addApplicationFramework nil
  (str "PigPen:"
       (or (some-> PigPenFunction
             (.getPackage)
             (.getImplementationVersion))
           "unknown")))

(defn cfields
  ^Fields [fields]
  {:pre [(seq fields)]}
  (->> fields
    (map str)
    (into-array String)
    (Fields.)))

(defn group-key-cfields
  #^"[Lcascading.tuple.Fields;" [keys join-nils?]
  (->> keys
    (map (fn [key]
           (let [^Fields fields (cfields [key])]
             (if join-nils?
               fields
               (doto fields
                 ;; side effect
                 (.setComparator
                   ^Comparable (str key)
                   ^Comparator (NullNotEquivalentComparator.)))))))
    (into-array Fields)))

(defn pipe-array
  "Type-hinted way to create pipe arrays"
  #^"[Lcascading.pipe.Pipe;" [pipes]
  (into-array Pipe pipes))

;; TODO use quoting instead of pr-str
(defn prepare-expr [expr]
  (case (:type expr)
    :field expr
    :code (-> expr
            (update-in [:init] pr-str)
            (update-in [:func] pr-str))))

(defn prepare-projection [p]
  (update-in p [:expr] prepare-expr))

(defn prepare-projections [ps]
  (mapv prepare-projection ps))

;; can these always be used for both load and store?
(defmulti get-tap :storage)

(defmethod get-tap :string [{:keys [^String location fields args]}]
  (Hfs. (TextLine. ^Fields (cfields (or fields args))) location))

(defmethod get-tap :tap [{:keys [opts]}]
  (get opts :tap))

(defmethod get-tap :default [{:keys [type]}]
  (throw (Exception. (str "Unrecognized tap type: " type))))

;; ******* Commands ********

(defmulti command->flowdef
  "Converts an individual command into the equivalent Cascading flow definition."
  (fn [{:keys [type]} ancestors flowdef] type))

(s/defmethod command->flowdef :load
  [{:keys [id fields], :as command} :- m/Load
   _
   ^FlowDef flowdef]
  (let [^Tap tap (get-tap command)
        pipe (->
               (Pipe. (str id))
               (Rename. (.getSourceFields tap) (cfields fields)))]
    ;; side effect
    (.addSource flowdef pipe tap)
    pipe))

(s/defmethod command->flowdef :store
  [command :- m/Store
   [{:keys [^Pipe pipe ancestor]}]
   ^FlowDef flowdef]
  (let [^Tap sink (get-tap command)
        fields (:fields ancestor)
        ; The tap needs the incoming field name to match (without the namespace
        ; added to all field symbols).
        pipe (if-let [tap-fields (seq (map symbol (.getSinkFields sink)))]
               (Rename. pipe (cfields fields) (cfields tap-fields))
               pipe)]
    ;; side effect
    (.addTailSink flowdef pipe sink)
    nil))

(s/defmethod command->flowdef :reduce
  [command :- m/Reduce
   [{:keys [^Pipe pipe]}]
   _]
  (GroupBy. pipe Fields/NONE))

(s/defmethod command->flowdef :reduce-fold
  [{:keys [reduce :- m/Reduce
           fold :- m/Project]}
   [{:keys [^Pipe pipe]}]
   _]
  (let [projections (:projections fold)
        context (pr-str `'{:projections ~(prepare-projections projections)})
        old-fields (cfields (get-in projections [0 :expr :args]))
        new-fields (cfields (get-in projections [0 :alias]))]
    (->
      (PigPenAggregateBy. context pipe Fields/NONE old-fields)
      (Rename. old-fields new-fields))))

(s/defmethod command->flowdef :group
  [{:keys [id keys fields join-types opts]} :- m/Group
   ancestors
   _]
  (let [join-keys (group-key-cfields keys (:join-nils opts))
        pipes (pipe-array
                (if (:join-nils opts)
                  (map :pipe ancestors)
                  ;; This is to induce pigpen's nil-joining behavior, which
                  ;; treats nils from the same relation as equal
                  (->> ancestors
                    (map-indexed
                      (fn [i a]
                        (let [fields (-> a :ancestor :fields cfields)]
                          (Each. ^Pipe (:pipe a)
                                 (InduceSentinelNils. (int i) fields))))))))]
    (CoGroup. (str id) pipes join-keys Fields/NONE (BufferJoin.))))

(s/defmethod command->flowdef :group-fold
  [{:keys [group :- m/Group
           fold :- m/Project]}
   ancestors
   _]
  (let [{:keys [id keys]} group
        join-keys (group-key-cfields keys (get-in group [:opts :join-nils]))
        ;; Perform all fold aggregations before co-grouping the relations
        pipes (pipe-array
                (zip [p (-> fold :projections next)
                      {:keys [pipe]} ancestors
                      key keys]
                  (if (some-> p :expr :udf #{:fold})
                    (let [context (pr-str `'{:projections ~(prepare-projections [p])})
                          group-fields (cfields [key])
                          arg-fields (cfields (get-in p [:expr :args]))]
                      (PigPenAggregateBy. context pipe group-fields arg-fields))
                    pipe)))]
    (CoGroup. (str id) pipes join-keys Fields/NONE (BufferJoin.))))

(s/defmethod command->flowdef :join
  [{:keys [id keys fields join-types opts]} :- m/Join
   ancestors
   _]
  (let [pipes (->> ancestors
                (map :pipe)
                (pipe-array))
        joiner (->> join-types
                 (map (comp boolean #{:required}))
                 (boolean-array)
                 (MixedJoin.))
        join-keys (group-key-cfields keys (:join-nils opts))]
    (CoGroup. (str id) pipes join-keys (cfields fields) joiner)))

(s/defmethod command->flowdef :project
  [{:keys [id projections fields]} :- m/Project
   [{:keys [^Pipe pipe ancestor]}]
   _]
  (case (:type ancestor)
    :reduce
    (let [context {:func   (prepare-projection (first projections))
                   :fields fields}]
      (Every. pipe (ReduceBuffer. (pr-str `'~context) (cfields fields)) Fields/RESULTS))

    (:group :group-fold)
    (let [;; the list of args required by this projection
          args (-> projections
                 first
                 (get-in [:expr :args]))

          ;; a list of which fields are required. This is to compute inner/outer groups
          required (mapcat (fn [a j] (when (= j :required) [a]))
                           (next args)
                           (:join-types ancestor))

          ;; folds add an extra layer of indirection to field names; this resolves it
          rename-fields (some->> ancestor
                          :fold
                          :projections
                          (map (fn [p]
                                 [(-> p :alias first)
                                  (or (-> p :expr :field)
                                      (-> p :expr :args first))]))
                          (into {}))

          ;; This exists becasue we use this for both fold and non-fold co-groupings.
          ;; For relations that have been folded already, there will only be one value,
          ;; so we take the first. This identifies which relations have been folded.
          folds (some->> ancestor
                  :fold
                  :projections
                  (filter (comp #{:fold} :udf :expr))
                  (map (comp first :alias))
                  (map rename-fields)
                  set)

          context {:args          args
                   :required      required
                   :rename-fields rename-fields
                   :folds         folds
                   :func          (prepare-projection (first projections))
                   :fields        fields}]

      (Every. pipe (GroupBuffer. (pr-str `'~context) (cfields fields)) Fields/RESULTS))

    ;else
    (let [field-projections (filter (comp #{:field} :type :expr) projections)
          funcs (filter (comp #{:code} :type :expr) projections)
          context {:field-projections field-projections
                   :func              (prepare-projection (first funcs))
                   :fields            fields}]

      (when (some :flatten field-projections)
        (throw (ex-info "Cascading doesn't support flattened projection fields"
                        {:fields fields})))

      (when (next funcs)
        (throw (ex-info "Cascading doesn't support multiple projection funcs"
                        {:funcs funcs})))

      (when-not (:flatten (first funcs))
        (throw (ex-info "Cascading doesn't support scalar funcs"
                        {:func (first funcs)})))

      (-> (Pipe. (str id) pipe)
        (Each. (PigPenFunction. (pr-str `'~context) (cfields fields)))))))

(s/defmethod command->flowdef :distinct
  [{:keys [fields]} :- m/Distinct
   [{:keys [^Pipe pipe ancestor]}]
   _]
  (-> pipe
    (Unique. Fields/ALL)
    (Rename. (cfields (:fields ancestor)) (cfields fields))))

(s/defmethod command->flowdef :take
  [{:keys [n fields]} :- m/Take
   [{:keys [^Pipe pipe ancestor]}]
   _]
  (-> pipe
    (Each. (Limit. n))
    (Rename. (cfields (:fields ancestor)) (cfields fields))))

(s/defmethod command->flowdef :sample
  [{:keys [p fields]} :- m/Sample
   [{:keys [^Pipe pipe ancestor]}]
   _]
  (-> pipe
    (Each. (Sample. p))
    (Rename. (cfields (:fields ancestor)) (cfields fields))))

(s/defmethod command->flowdef :concat
  [{:keys [fields]} :- m/Concat
   ancestors
   _]
  (->> ancestors
    (map (fn [{:keys [^Pipe pipe ancestor]}]
           (Rename. pipe (cfields (:fields ancestor)) (cfields fields))))
    (into-array Pipe)
    (Merge.)))

(s/defmethod command->flowdef :sort
  [{:keys [key comp fields]} :- m/Sort
   [{:keys [^Pipe pipe ancestor]}]
   _]
  (let [reverse-order? (= :desc comp)]
    (-> pipe
      (GroupBy. Fields/NONE (cfields [key]) reverse-order?)
      ;; TODO is there a way to rename and select a single field at the same time?
      (Rename. (cfields (next (:fields ancestor))) (cfields fields))
      (Each. (cfields fields) (Identity.) Fields/RESULTS))))

(s/defmethod command->flowdef :rank
  [{:keys [id ancestors fields]} :- m/Rank
   [{:keys [^Pipe pipe]}]
   _]
  ; TODO: In this naive, single-reducer implementation, a rank followed by an
  ; sort should skip the sort since rank does a group-by itself.
  (-> pipe
    (GroupBy. Fields/NONE)
    (Every. (RankBuffer. (cfields fields)) Fields/RESULTS)))

(s/defmethod command->flowdef :store-many
  [_ _ _]
  ; No-op, since the flowdef already contains everything needed to handle multiple outputs.
  nil)

(s/defmethod command->flowdef :noop
  [{:keys [id fields]} :- m/NoOp
   [{:keys [^Pipe pipe ancestor]}]
   _]
  (->
    (Pipe. (str id) pipe)
    (Rename. (cfields (:fields ancestor)) (cfields fields))))

(defmethod command->flowdef :default
  [command _ _]
  (throw (Exception. (str "Command " (:type command) " not implemented yet for Cascading!"))))

(defn command->flowdef+
  [[flowdef pipes] {:keys [id ancestors], :as command}]
  (let [pipe (command->flowdef command (map pipes ancestors) flowdef)]
    [flowdef (assoc pipes id {:ancestor command, :pipe pipe})]))

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [^FlowConnector connector commands]
  (let [[flowdef _] (reduce command->flowdef+ [(FlowDef/flowDef) {}] commands)]
    (.connect connector flowdef)))

