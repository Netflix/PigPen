(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine)
           (cascading.pipe Pipe Each CoGroup Every)
           (cascading.flow.hadoop HadoopFlowConnector)
           (pigpen.cascading PigPenFunction GroupBuffer JoinBuffer)
           (cascading.tuple Fields)
           (cascading.operation Identity Insert)
           (cascading.pipe.joiner OuterJoin BufferJoin InnerJoin LeftJoin RightJoin)
           (cascading.pipe.assembly Unique)
           (cascading.operation.filter Limit Sample FilterNull)
           (cascading.util NullNotEquivalentComparator)
           (cascading.flow FlowConnector))
  (:require [pigpen.runtime :as rt]
            [pigpen.cascading.runtime :as cs]
            [pigpen.raw :as raw]
            [pigpen.oven :as oven]
            [taoensso.nippy :refer [freeze thaw]]
            [clojure.pprint]))

(defn- add-val [flowdef path id val]
  (update-in flowdef path (partial merge {id val})))

(defn- cfields [fields]
  {:pre [(not-empty fields)]}
  (Fields. (into-array (map str fields))))

(defn- cascading-field [name-or-number]
  (if (number? name-or-number)
    (int name-or-number)
    (str name-or-number)))

(defn- group-key-cfields [keys join-nils]
  (into-array (map #(let [f (cfields %)]
                     (when-not join-nils (.setComparator f (str (first %)) (NullNotEquivalentComparator.)))
                     f) keys)))

(defmulti get-tap-fn
          identity)

(defmethod get-tap-fn :string [_]
  (fn [location opts] (Hfs. (TextLine.) location)))

(defmethod get-tap-fn :default [_]
  (throw (Exception. (str "Unrecognized tap type: " name))))

(defn load-text [location]
  (-> (raw/load$ location '[offset line] :string {})))

(defn load-tsv
  ([location] (load-tsv location "\t"))
  ([location delimiter]
    (-> (load-text location)
        (raw/bind$
          `(rt/map->bind (fn [~'offset ~'line] (pigpen.extensions.core/structured-split ~'line ~delimiter)))
          {:args          '[offset line]
           :field-type-in :native}))))

(defn load-clj [location]
  (-> (load-text location)
      (raw/bind$ '[clojure.edn]
                 `(rt/map->bind (fn [~'offset ~'line] (clojure.edn/read-string ~'line)))
                 {:args          '[offset line]
                  :field-type-in :native})))

;; ******* Commands ********

(defmulti command->flowdef
          "Converts an individual command into the equivalent Cascading flow definition."
          (fn [{:keys [type]} flowdef] type))


(defmethod command->flowdef :load
  [{:keys [id location storage fields opts]} flowdef]
  {:pre [id location storage fields]}
  (let [pipe (Pipe. (str id))]
    (-> flowdef
        (add-val [:sources] id ((get-tap-fn storage) location opts))
        (add-val [:pipes] id pipe))))

(defmethod command->flowdef :store
  [{:keys [id ancestors location storage opts]} flowdef]
  {:pre [id ancestors location storage]}
  (-> flowdef
      (add-val [:pipe-to-sink] (first ancestors) id)
      (add-val [:sinks] id ((get-tap-fn storage) location opts))))

(defmethod command->flowdef :code-def
  [{:keys [code-defs pipe field-projections]} flowdef]
  {:pre [code-defs (= (count (distinct (map :udf code-defs))) 1)]}
  (let [inits (map #(str (get-in % [:expr :init])) code-defs)
        funcs (map #(str (get-in % [:expr :func])) code-defs)
        udf (first (map :udf code-defs))
        fields (if field-projections
                 (cfields (map #(cascading-field (:alias %)) field-projections))
                 Fields/UNKNOWN)
        cogroup-opts (get-in flowdef [:cogroup-opts pipe])]
    (if-not (nil? cogroup-opts)
      (let [buffer (case (:group-type cogroup-opts)
                     :group (GroupBuffer. inits funcs fields (:num-streams cogroup-opts) (:group-all cogroup-opts) (:join-nils cogroup-opts) (:join-requirements cogroup-opts) udf)
                     :join (JoinBuffer. (first inits) (first funcs) fields (:all-args cogroup-opts)))]
        (update-in flowdef [:pipes pipe] #(Every. % buffer Fields/RESULTS)))
      (update-in flowdef [:pipes pipe] #(Each. % (PigPenFunction. (first inits) (first funcs) fields) Fields/RESULTS)))))

(defmethod command->flowdef :group
  [{:keys [id keys fields join-types ancestors opts]} flowdef]
  {:pre [id keys fields join-types ancestors]}
  (let [is-group-all (= keys [:pigpen.raw/group-all])
        keys (if is-group-all [["group_all"]] keys)
        pipes (if is-group-all
                [(Each. ((:pipes flowdef) (first ancestors))
                        (Insert. (cfields ["group_all"]) (into-array [1]))
                        (cfields ["group_all" "value"]))]
                (map (:pipes flowdef) ancestors))
        is-inner (every? #{:required} join-types)
        pipes (map (fn [p k] (if is-inner
                               (Each. p (cfields k) (FilterNull.))
                               p))
                   pipes keys)]
    (-> flowdef
        (add-val [:pipes] id (CoGroup. (str id)
                                       (into-array Pipe pipes)
                                       (into-array (map cfields keys))
                                       Fields/NONE
                                       (BufferJoin.)))
        (add-val [:cogroup-opts] id {:group-id          id
                                     :group-type        :group
                                     :join-nils         (true? (:join-nils opts))
                                     :group-all         is-group-all
                                     :num-streams       (count pipes)
                                     :join-requirements (map #(= :required %) join-types)}))))

(defmethod command->flowdef :join
  [{:keys [id keys fields join-types ancestors opts]} flowdef]
  {:pre [id keys fields join-types ancestors opts]}
  (let [join-nils (:join-nils opts)
        joiner (case join-types
                 [:required :required] (InnerJoin.)
                 [:required :optional] (LeftJoin.)
                 [:optional :required] (RightJoin.)
                 [:optional :optional] (OuterJoin.))]
    (-> flowdef
        (add-val [:pipes] id (CoGroup. (str id)
                                       ;; TODO: adding an Each with Identity here is a hack around a possible bug in Cascading involving self-joins.
                                       (into-array Pipe (map-indexed (fn [i a] (let [p ((:pipes flowdef) a)
                                                                                     p (Pipe. (str (nth fields (* i 2))) p)
                                                                                     p (Each. p (Identity.))]
                                                                                 p)) ancestors))
                                       (group-key-cfields keys join-nils)
                                       (cfields fields)
                                       joiner))
        (add-val [:cogroup-opts] id {:group-id   id
                                     :group-type :join
                                     :all-args   (true? (:all-args opts))}))))

(defmethod command->flowdef :projection-flat
  [{:keys [code alias pipe field-projections]} flowdef]
  {:pre [code alias]}
  (command->flowdef (assoc code :pipe pipe
                                :field-projections (or field-projections [{:alias alias}])) flowdef))

(defmethod command->flowdef :projection-func
  [{:keys [code alias pipe field-projections]} flowdef]
  {:pre [code alias]}
  (command->flowdef (assoc code :pipe pipe
                                :field-projections (or field-projections [{:alias alias}])) flowdef))

(defmethod command->flowdef :generate
  [{:keys [id ancestors projections field-projections opts]} flowdef]
  {:pre [id (= 1 (count ancestors)) (not-empty projections)]}

  ;; TODO: this should never occur once everything is implemented.
  (when-not (contains? (:pipes flowdef) (first ancestors))
    (do
      (println "flowdef" flowdef)
      (println "id" id)
      (println "ancestors" ancestors)
      (throw (Exception. "not implemented"))))

  (let [ancestor (first ancestors)
        pipe ((:pipes flowdef) ancestor)
        code-defs (map :code (filter #(let [t (:type %)]
                                       (or (= :projection-flat t) (= :projection-func t))) projections))
        field-projections (if (some #(let [t (:type %)] (= :projection-field t)) projections)
                            projections
                            field-projections)
        flowdef (add-val flowdef [:pipes] id (Pipe. (str id) pipe))
        flowdef (let [pipe-opts (get-in flowdef [:cogroup-opts ancestor])]
                  (if (= (:group-id pipe-opts) ancestor)
                    (add-val flowdef [:cogroup-opts] id pipe-opts)
                    flowdef))
        flowdef (command->flowdef {:type              :code-def
                                   :pipe              id
                                   :field-projections field-projections
                                   :code-defs         code-defs}
                                  flowdef)]
    flowdef))

(defmethod command->flowdef :distinct
  [{:keys [id ancestors]} flowdef]
  {:pre [id (= 1 (count ancestors))]}
  (let [pipe ((:pipes flowdef) (first ancestors))]
    (add-val flowdef [:pipes] id (Unique. pipe Fields/ALL))))

(defmethod command->flowdef :limit
  [{:keys [id n ancestors]} flowdef]
  {:pre [id n (= 1 (count ancestors))]}
  (let [pipe ((:pipes flowdef) (first ancestors))]
    (add-val flowdef [:pipes] id (Each. pipe (Limit. n)))))

(defmethod command->flowdef :sample
  [{:keys [id p ancestors]} flowdef]
  {:pre [id p (= 1 (count ancestors))]}
  (let [pipe ((:pipes flowdef) (first ancestors))]
    (add-val flowdef [:pipes] id (Each. pipe (Sample. p)))))

(defmethod command->flowdef :script
  [_ flowdef]
  ; No-op, since the flowdef already contains everything needed to handle multiple outputs.
  flowdef)

(defmethod command->flowdef :default
  [command _]
  (throw (Exception. (str "Command " (:type command) " not implemented yet for Cascading!"))))

(defn- collapse-field-projections [commands]
  (let [is-field-projection (fn [c] (every? #(= :projection-field (:type %)) (if-let [p (:projections c)] p [0])))
        ;(let [is-field-projection #(= :projection-field (get-in % [:projections 0 :type]))
        fp-by-key (fn [key-fn] (->> commands
                                    (filter is-field-projection)
                                    (map (fn [c] [(key-fn c) c]))
                                    (into {})))
        fp-by-ancestor (fp-by-key #(first (:ancestors %)))
        fp-by-id (fp-by-key :id)]
    (->> commands
         (map (fn [c]
                (let [fp (fp-by-ancestor (:id c))
                      c (if fp
                          (assoc c :field-projections (:projections fp))
                          c)]
                  (update-in c [:ancestors] (fn [a] (map #(if (fp-by-id %)
                                                           (first (:ancestors (fp-by-id %)))
                                                           %) a))))))
         (remove is-field-projection))))


(defn preprocess-commands [commands]
  (-> commands
      collapse-field-projections))

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [commands ^FlowConnector connector]
  (clojure.pprint/pprint (preprocess-commands commands))
  (let [flowdef (reduce (fn [def cmd] (command->flowdef cmd def)) {} (preprocess-commands commands))
        {:keys [sources pipe-to-sink sinks pipes]} flowdef
        sources-map (into {} (map (fn [s] [(str s) (sources s)]) (keys sources)))
        sinks-map (into {} (map (fn [[p s]] [(str p) (sinks s)]) pipe-to-sink))
        tail-pipes (into-array Pipe (map #(pipes %) (keys pipe-to-sink)))]
    (.connect connector sources-map sinks-map tail-pipes)))

(defn generate-flow
  "Transforms the relation specified into a Cascading flow that is ready to be executed."
  ([query] (generate-flow (HadoopFlowConnector.) query))
  ([connector query]
    (-> query
        (oven/bake :cascading {} {})
        (commands->flow connector))))
