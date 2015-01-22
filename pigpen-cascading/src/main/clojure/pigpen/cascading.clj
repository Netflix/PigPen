(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine)
           (cascading.pipe Pipe Each CoGroup Every Merge GroupBy)
           (cascading.flow.hadoop HadoopFlowConnector)
           (pigpen.cascading PigPenFunction GroupBuffer JoinBuffer PigPenAggregateBy OperationUtil RankBuffer)
           (cascading.tuple Fields)
           (cascading.operation Identity Insert)
           (cascading.pipe.joiner OuterJoin BufferJoin InnerJoin LeftJoin RightJoin)
           (cascading.pipe.assembly Unique Rename)
           (cascading.operation.filter Limit Sample FilterNull)
           (cascading.util NullNotEquivalentComparator)
           (cascading.flow FlowConnector))
  (:require [pigpen.oven :as oven]
            [clojure.pprint]))

(defn- add-val [flowdef path id val]
  (update-in flowdef path #(merge % {id val})))

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
  (fn [location opts] (Hfs. (TextLine. (cfields ["line"])) location)))

(defmethod get-tap-fn :default [t]
  (throw (Exception. (str "Unrecognized tap type: " t))))

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

(defn- partial-aggregation-code
  [pipe-id inits funcs cogroup-opts field-projections flowdef]
  (let [key-fields (if (:group-all cogroup-opts)
                     Fields/NONE
                     (cfields (first (:keys cogroup-opts))))
        ; This is a hack to emulate multiple streams with a single stream by using a sentinel value to fill absent fields.
        ; For example, if we have generate1 -> [1 1 2 3], generate2 => [1 2 2 3], the resulting merged stream exposed  by
        ; cascading will be:
        ; [[1 1 s] [1 1 s] [2 2 s] [3 3 s] [1 s 1] [2 s 2] [2 s 2] [3 s 3]], where each tuple contains [key, value1, value2],
        ; and s represents the sentinel value.
        ; Another twist is that in case of a self join, a single pipe must be used because otherwise cascading will
        ; do each operation twice.
        val-fields (->> field-projections
                        (filter #(= :projection-func (:type %)))
                        (map :alias)
                        (map str))
        field-to-source (zipmap val-fields (:ancestors cogroup-opts))
        pipes (->> (:ancestors cogroup-opts)
                   (map (:pipes flowdef))
                   (map-indexed (fn [index pipe]
                                  (Rename. pipe (cfields ["value"]) (cfields [(nth val-fields index)]))))
                   (map-indexed (fn [index pipe]
                                  (reduce (fn [pipe {:keys [field shared-source]}]
                                            (if shared-source
                                              ; Here we're assigning a different name to the same actual field, since the
                                              ; source pipe is the same.
                                              (Each. pipe (cfields [(nth val-fields index)]) (Identity. (cfields [field])) Fields/ALL)

                                              ; Here we use the sentinel to ensure any tuples from this pipe are discarded when
                                              ; processed by functions meant for other pipes.
                                              (Each. pipe (Insert. (cfields [field]) (into-array [OperationUtil/SENTINEL_VALUE])) Fields/ALL)))
                                          pipe
                                          (keep-indexed (fn [i v]
                                                          (if (= index i)
                                                            nil
                                                            {:field         v
                                                             :shared-source (= (.getName pipe)
                                                                               (str (field-to-source (nth val-fields i))))}))
                                                        val-fields))))
                   ; Only keep one pipe per source.
                   (reduce (fn [pipes pipe]
                             (let [pipe-names (into #{} (map #(.getName %) pipes))]
                               (if (pipe-names (.getName pipe))
                                 pipes
                                 (conj pipes pipe))))
                           [])
                   (into-array))]
    (add-val flowdef [:pipes] pipe-id (PigPenAggregateBy/buildAssembly (str pipe-id) pipes key-fields val-fields inits funcs))))

(defn- code->flowdef
  [{:keys [code-defs pipe-id field-projections]} flowdef]
  {:pre [code-defs (= (count (distinct (map :udf code-defs))) 1)]}
  (let [inits (mapv #(str (get-in % [:expr :init])) code-defs)
        funcs (mapv #(str (get-in % [:expr :func])) code-defs)
        udf (first (map :udf code-defs))
        field-names (if field-projections
                      (mapv #(cascading-field (:alias %)) field-projections)
                      ; TODO: this field name should not be hardcoded
                      ["value"])
        fields (cfields field-names)
        cogroup-opts (get-in flowdef [:cogroup-opts pipe-id])]
    (cond (nil? cogroup-opts)
          (update-in flowdef [:pipes pipe-id] #(Each. % (PigPenFunction. (first inits) (first funcs) fields) Fields/RESULTS))

          (= udf :algebraic)
          (partial-aggregation-code pipe-id inits funcs cogroup-opts field-projections flowdef)

          :else
          (let [key-separate-from-value (> (count (first (map :args code-defs))) (:num-streams cogroup-opts))
                buffer (case (:group-type cogroup-opts)
                         :group (GroupBuffer. (first inits)
                                              (first funcs)
                                              fields
                                              (:num-streams cogroup-opts)
                                              (:group-all cogroup-opts)
                                              (:join-nils cogroup-opts)
                                              (:join-requirements cogroup-opts)
                                              key-separate-from-value)
                         :join (JoinBuffer. (first inits) (first funcs) fields (:all-args cogroup-opts)))]
            (update-in flowdef [:pipes pipe-id] #(Every. % buffer Fields/RESULTS))))))

(defn- partial-aggregation
  [id is-group-all keys ancestors flowdef]
  (-> flowdef
      (add-val [:pipes] id (Pipe. (str id)))
      (add-val [:cogroup-opts] id {:group-id   id
                                   :group-type :partial-aggregation
                                   :keys       keys
                                   :ancestors  ancestors
                                   :group-all  is-group-all})))

(defn- cogroup
  [id is-group-all keys ancestors join-types opts flowdef]
  (let [pipes (map (:pipes flowdef) ancestors)
        is-inner (every? #{:required} join-types)
        pipes (map (fn [p k] (if is-inner
                               (Each. p (cfields k) (FilterNull.))
                               p))
                   pipes keys)]
    (-> flowdef
        (add-val [:pipes] id (CoGroup. (str id)
                                       (into-array Pipe pipes)
                                       (into-array (map (fn [k] (if is-group-all
                                                                  Fields/NONE
                                                                  (cfields k))) keys))
                                       Fields/NONE
                                       (BufferJoin.)))
        (add-val [:cogroup-opts] id {:group-id          id
                                     :group-type        :group
                                     :join-nils         (true? (:join-nils opts))
                                     :group-all         is-group-all
                                     :num-streams       (count pipes)
                                     :join-requirements (map #(= :required %) join-types)}))))

(defmethod command->flowdef :group
  [{:keys [id keys fields join-types ancestors requires-partial-aggregation opts]} flowdef]
  {:pre [id keys fields join-types ancestors]}
  (let [is-group-all (= keys [:pigpen.raw/group-all])
        keys (if is-group-all [["pigpen.raw/group-all"]] keys)]
    (if requires-partial-aggregation
      (partial-aggregation id is-group-all keys ancestors flowdef)
      (cogroup id is-group-all keys ancestors join-types opts flowdef))))

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
        (add-val [:cogroup-opts] id {:group-id    id
                                     :group-type  :join
                                     :num-streams (count ancestors)
                                     :all-args    (true? (:all-args opts))}))))

(defmethod command->flowdef :projection-flat
  [{:keys [code alias pipe-id field-projections]} flowdef]
  {:pre [code alias]}
  (command->flowdef (assoc code :pipe-id pipe-id
                                :field-projections (or field-projections [{:alias alias}])) flowdef))

(defmethod command->flowdef :projection-func
  [{:keys [code alias pipe-id field-projections]} flowdef]
  {:pre [code alias]}
  (command->flowdef (assoc code :pipe-id pipe-id
                                :field-projections (or field-projections [{:alias alias}])) flowdef))

(defmethod command->flowdef :generate
  [{:keys [id ancestors projections field-projections opts]} flowdef]
  {:pre [id (= 1 (count ancestors)) (not-empty projections)]}
  (let [ancestor (first ancestors)
        pipe ((:pipes flowdef) ancestor)
        code-defs (map :code (filter #(let [t (:type %)]
                                       (or (= :projection-flat t) (= :projection-func t))) projections))
        field-projections (if (some #(let [t (:type %)] (or (= :projection-field t) (= :projection-func t))) projections)
                            projections
                            field-projections)
        flowdef (add-val flowdef [:pipes] id (Pipe. (str id) pipe))
        flowdef (let [pipe-opts (get-in flowdef [:cogroup-opts ancestor])]
                  (if (= (:group-id pipe-opts) ancestor)
                    (add-val flowdef [:cogroup-opts] id pipe-opts)
                    flowdef))
        flowdef (code->flowdef {:pipe-id           id
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

(defmethod command->flowdef :union
  [{:keys [id ancestors opts]} flowdef]
  {:pre [id ancestors]}
  (let [union-pipe (Merge. (into-array (map (:pipes flowdef) ancestors)))]
    (add-val flowdef [:pipes] id union-pipe)))

(defmethod command->flowdef :order
  [{:keys [id ancestors sort-keys fields opts]} flowdef]
  {:pre [id (= 1 (count ancestors))]}
  (let [[key type] sort-keys
        reverse-order (= :desc type)
        sort-pipe (GroupBy. ((:pipes flowdef) (first ancestors)) Fields/NONE (cfields [key]) reverse-order)
        sort-pipe (Each. sort-pipe (cfields fields) (Identity.))]
    (add-val flowdef [:pipes] id sort-pipe)))

(defmethod command->flowdef :rank
  [{:keys [id ancestors fields opts]} flowdef]
  {:pre [id (= 1 (count ancestors))]}
  ; TODO: In this naive, single-reducer implementation, a rank followed by an
  ; order should skip the order since rank does a group-by itself.
  (let [val-fields (cfields fields)
        rank-pipe (GroupBy. ((:pipes flowdef) (first ancestors)) Fields/NONE)
        rank-pipe (Every. rank-pipe (RankBuffer. val-fields) Fields/RESULTS)]
    (add-val flowdef [:pipes] id rank-pipe)))

(defmethod command->flowdef :script
  [_ flowdef]
  ; No-op, since the flowdef already contains everything needed to handle multiple outputs.
  flowdef)

(defmethod command->flowdef :default
  [command _]
  (throw (Exception. (str "Command " (:type command) " not implemented yet for Cascading!"))))

(defn- collapse-field-projections [commands]
  (let [is-field-projection (fn [c] (every? #(= :projection-field (:type %)) (if-let [p (:projections c)] p [0])))
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

(defn- label-partial-aggregation-groups [commands]
  (let [cmd-by-id (->> commands
                       (map (fn [c] [(:id c) c]))
                       (into {}))
        group-to-successor (->> commands
                                (filter #(= :group (-> (:ancestors %)
                                                       (first)
                                                       cmd-by-id
                                                       :type)))
                                (map (fn [c] [(first (:ancestors c)) (:id c)]))
                                (into {}))]
    (map (fn [c]
           (if (and (= :group (:type c))
                    (some #(= :projection-func (:type %)) (-> (:id c) (group-to-successor) (cmd-by-id) :projections)))
             (assoc c :requires-partial-aggregation true)
             c))
         commands)))

(defn preprocess-commands [commands]
  (-> commands
      collapse-field-projections
      label-partial-aggregation-groups))

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [commands ^FlowConnector connector]
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
