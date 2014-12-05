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
           (cascading.operation.filter Limit Sample)
           (cascading.util NullNotEquivalentComparator))
  (:require [pigpen.runtime :as rt]
            [pigpen.cascading.runtime :as cs]
            [pigpen.raw :as raw]
            [pigpen.oven :as oven]
            [taoensso.nippy :refer [freeze thaw]]
            [clojure.pprint]))

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
        (update-in [:sources] (partial merge {id ((get-tap-fn storage) location opts)}))
        (update-in [:pipes] (partial merge {id pipe})))))

(defmethod command->flowdef :store
  [{:keys [id ancestors location storage opts]} flowdef]
  {:pre [id ancestors location storage]}
  (-> flowdef
      (update-in [:pipe-to-sink] (partial merge {(first ancestors) id}))
      (update-in [:sinks] (partial merge {id ((get-tap-fn storage) location opts)}))))

(defn- cascading-field [name-or-number]
  (if (number? name-or-number)
    (int name-or-number)
    (str name-or-number)))

(defmethod command->flowdef :code
  [{:keys [expr args pipe field-projections]} flowdef]
  {:pre [expr args]}
  (let [{:keys [init func]} expr
        fields (if field-projections
                 (Fields. (into-array (map #(cascading-field (:alias %)) field-projections)))
                 Fields/UNKNOWN)
        get-group-info #(cond (instance? CoGroup %) {:num-streams (alength (.getPrevious %))
                                                     :type        (if (.startsWith (.getName %) "join") :join :group)
                                                     :all-args    (.contains (.getName %) "all-args")
                                                     :group-all   (.contains (.getName %) "group-all")}
                              (or (nil? %) (instance? Every %)) nil
                              :else (recur (first (.getPrevious %))))
        group-info (get-group-info (get-in flowdef [:pipes pipe]))]
    (if-not (nil? group-info)
      (let [buffer (case (:type group-info)
                     :group (GroupBuffer. (str init) (str func) fields (:num-streams group-info) (:group-all group-info))
                     :join (JoinBuffer. (str init) (str func) fields (:all-args group-info)))]
        (update-in flowdef [:pipes pipe] #(Every. % buffer Fields/RESULTS)))
      (update-in flowdef [:pipes pipe] #(Each. % (PigPenFunction. (str init) (str func) fields) Fields/RESULTS)))))

(defmethod command->flowdef :group
  [{:keys [id keys fields join-types ancestors opts]} flowdef]
  {:pre [id keys fields join-types ancestors]}
  (let [is-group-all (= keys [:pigpen.raw/group-all])
        keys (if is-group-all [["group_all"]] keys)
        pipes (if is-group-all
                [(Each. ((:pipes flowdef) (first ancestors))
                        (Insert. (Fields. (into-array ["group_all"])) (into-array [1]))
                        (Fields. (into-array ["group_all" "value"])))]
                (map (:pipes flowdef) ancestors))]
    (update-in flowdef [:pipes] (partial merge {id (CoGroup. (str id (if is-group-all "group-all" ""))
                                                             (into-array Pipe pipes)
                                                             (into-array (map #(Fields. (into-array (map str %)))
                                                                              keys))
                                                             Fields/NONE
                                                             (BufferJoin.))}))))

(defmethod command->flowdef :join
  [{:keys [id keys fields join-types ancestors opts]} flowdef]
  {:pre [id keys fields join-types ancestors opts]}
  (let [join-nils (:join-nils opts)
        all-args (if (:all-args opts) "all-args" "")
        joiner (case join-types
                 [:required :required] (InnerJoin.)
                 [:required :optional] (LeftJoin.)
                 [:optional :required] (RightJoin.)
                 [:optional :optional] (OuterJoin.))]
    (update-in flowdef [:pipes] (partial merge {id (CoGroup. (str id all-args) ;; TODO: find a less hacky way to pass the all-args flag
                                                             ;; TODO: adding an Each with Identity here is a hack around a possible bug in Cascading involving self-joins.
                                                             (into-array Pipe (map-indexed (fn [i a] (let [p ((:pipes flowdef) a)
                                                                                                           p (Pipe. (str (nth fields (* i 2))) p)
                                                                                                           p (Each. p (Identity.))]
                                                                                                       p)) ancestors))
                                                             (into-array (map #(let [f (Fields. (into-array (map str %)))]
                                                                                (when-not join-nils (.setComparator f (str (first %)) (NullNotEquivalentComparator.)))
                                                                                f) keys))
                                                             (Fields. (into-array (map str fields)))
                                                             joiner)}))))

(defmethod command->flowdef :projection-flat
  [{:keys [code alias pipe field-projections]} flowdef]
  {:pre [code alias]}
  (command->flowdef (assoc code :pipe pipe
                           :field-projections (or field-projections [{:alias alias}])) flowdef))

(defmethod command->flowdef :generate
  [{:keys [id ancestors projections field-projections opts]} flowdef]
  {:pre [id (= 1 (count ancestors)) (not-empty projections)]}
  (let [ancestor (first ancestors)
        new-flowdef (cond (contains? (:pipes flowdef) ancestor) (let [pipe ((:pipes flowdef) ancestor)]
                                                                  (-> flowdef
                                                                      (update-in [:pipes] (partial merge {id (Pipe. (str id) pipe)}))))
                          :else (do
                                  (println "flowdef" flowdef)
                                  (println "id" id)
                                  (println "ancestors" ancestors)
                                  (throw (Exception. "not implemented"))))
        flat-projections (filter #(= :projection-flat (:type %)) projections)
        new-flowdef (reduce (fn [def cmd] (command->flowdef
                                            (assoc cmd :pipe id
                                                   :field-projections field-projections)
                                            def))
                            new-flowdef
                            flat-projections)]
    new-flowdef))

(defmethod command->flowdef :distinct
  [{:keys [id ancestors]} flowdef]
  {:pre [id (= 1 (count ancestors))]}
  (let [pipe ((:pipes flowdef) (first ancestors))]
    (update-in flowdef [:pipes] (partial merge {id (Unique. pipe Fields/ALL)}))))

(defmethod command->flowdef :limit
  [{:keys [id n ancestors]} flowdef]
  {:pre [id n (= 1 (count ancestors))]}
  (let [pipe ((:pipes flowdef) (first ancestors))]
    (update-in flowdef [:pipes] (partial merge {id (Each. pipe (Limit. n))}))))

(defmethod command->flowdef :sample
  [{:keys [id p ancestors]} flowdef]
  {:pre [id p (= 1 (count ancestors))]}
  (let [pipe ((:pipes flowdef) (first ancestors))]
    (update-in flowdef [:pipes] (partial merge {id (Each. pipe (Sample. p))}))))

(defmethod command->flowdef :script
  [_ flowdef]
  ; No-op, since the flowdef already contains everything needed to handle multiple outputs.
  flowdef)

(defmethod command->flowdef :default
  [command _]
  (throw (Exception. (str "Command " (:type command) " not implemented yet for Cascading!"))))

(defn preprocess-commands [commands]
  (let [is-field-projection #(= :projection-field (get-in % [:projections 0 :type]))
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

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [commands]
  (clojure.pprint/pprint (preprocess-commands commands))
  (let [flowdef (reduce (fn [def cmd] (command->flowdef cmd def)) {} (preprocess-commands commands))
        {:keys [sources pipe-to-sink sinks pipes]} flowdef
        sources-map (into {} (map (fn [s] [(str s) (sources s)]) (keys sources)))
        sinks-map (into {} (map (fn [[p s]] [(str p) (sinks s)]) pipe-to-sink))
        tail-pipes (into-array Pipe (map #(pipes %) (keys pipe-to-sink)))]
    (.connect (HadoopFlowConnector.) sources-map sinks-map tail-pipes)))

(defn generate-flow
  "Transforms the relation specified into a Cascading flow that is ready to be executed."
  ([query] (generate-flow {} query))
  ([opts query]
    (-> query
        (oven/bake :cascading {} opts)
        commands->flow)))
