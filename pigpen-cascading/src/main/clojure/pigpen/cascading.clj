(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine)
           (cascading.pipe Pipe Each CoGroup Every)
           (cascading.flow.hadoop HadoopFlowConnector)
           (pigpen.cascading PigPenFunction GroupBuffer JoinBuffer)
           (cascading.tuple Fields)
           (cascading.operation Identity)
           (cascading.pipe.joiner OuterJoin BufferJoin InnerJoin LeftJoin RightJoin)
           (org.apache.hadoop.io BytesWritable)
           (cascading.pipe.assembly Unique))
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
      (raw/bind$
        `(rt/map->bind (fn [~'offset ~'line] (clojure.edn/read-string ~'line)))
        {:args          '[offset line]
         :field-type-in :native})))

(defn store-text [location f relation]
  (-> relation
      (raw/bind$ `(rt/map->bind ~f)
                 {:args (:fields relation), :field-type-out :native})
      (raw/store$ location :string {})))

(defn store-tsv
  ([location relation] (store-tsv location "\t" relation))
  ([location delimiter relation]
   (store-text location `(fn [~'s] (clojure.string/join ~delimiter (map print-str ~'s))) relation)))

(defn store-clj [location relation]
  (store-text location `clojure.core/pr-str relation))

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
                                                     :type        (if (.startsWith (.getName %) "join") :join :group)}
                              (or (nil? %) (instance? Every %)) nil
                              :else (recur (first (.getPrevious %))))]
    (let [group-info (get-group-info (get-in flowdef [:pipes pipe]))]
      (if-not (nil? group-info)
        (let [buffer ({:group (GroupBuffer. (str init) (str func) fields (:num-streams group-info))
                       :join  (JoinBuffer. (str init) (str func) fields)}
                      (:type group-info))]
          (update-in flowdef [:pipes pipe] #(Every. % buffer Fields/RESULTS)))
        (update-in flowdef [:pipes pipe] #(Each. % (PigPenFunction. (str init) (str func) fields) Fields/RESULTS))))))

(defmethod command->flowdef :group
  [{:keys [id keys fields join-types ancestors opts]} flowdef]
  {:pre [id keys fields join-types ancestors]}
  (update-in flowdef [:pipes] (partial merge {id (CoGroup. (str id)
                                                           (into-array Pipe (map (:pipes flowdef) ancestors))
                                                           (into-array (map #(Fields. (into-array (map str %))) keys))
                                                           Fields/NONE
                                                           (BufferJoin.))})))

(defmethod command->flowdef :join
  [{:keys [id keys fields join-types ancestors]} flowdef]
  {:pre [id keys fields join-types ancestors]}
  (let [joiner ({[:required :required] (InnerJoin.)
                 [:required :optional] (LeftJoin.)
                 [:optional :required] (RightJoin.)
                 [:optional :optional] (OuterJoin.)} join-types)]
    (update-in flowdef [:pipes] (partial merge {id (CoGroup. (str id)
                                                             (into-array Pipe (map (:pipes flowdef) ancestors))
                                                             (into-array (map #(Fields. (into-array (map str %))) keys))
                                                             (Fields. (into-array (map str fields)))
                                                             joiner)}))))

(defmethod command->flowdef :projection-flat
  [{:keys [code alias pipe field-projections]} flowdef]
  {:pre [code alias]}
  (command->flowdef (assoc code :pipe pipe :field-projections (or field-projections [{:alias alias}])) flowdef))

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
        new-flowdef (reduce (fn [def cmd] (command->flowdef (assoc cmd :pipe id :field-projections field-projections) def))
                            new-flowdef
                            flat-projections)]
    new-flowdef))

(defmethod command->flowdef :distinct
  [{:keys [id ancestors]} flowdef]
  {:pre [id (= 1 (count ancestors))]}
  (let [pipe ((:pipes flowdef) (first ancestors))]
    (update-in flowdef [:pipes] (partial merge {id (Unique. pipe Fields/ALL)}))))

(defmethod command->flowdef :script
  [_ flowdef]
  ; No-op, since the flowdef already contains everything needed to handle multiple outputs.
  flowdef)

(defmethod command->flowdef :default
  [command _]
  (throw (Exception. (str "Command " (:type command) " not implemented yet for Cascading!"))))

(defn preprocess-commands [commands]
  (let [is-field-projection #(= :projection-field (get-in % [:projections 0 :type]))
        ancestors-map (->> commands
                           (filter is-field-projection)
                           (map (fn [c] [(:id c) (first (:ancestors c))]))
                           (into {}))]
    (->> commands
         (map-indexed (fn [i c]
                        (let [c (if (is-field-projection (get commands (+ i 1)))
                                  (assoc c :field-projections (get-in (get commands (+ i 1)) [:projections]))
                                  c)]
                          (update-in c [:ancestors] (fn [a] (map #(if (contains? ancestors-map %)
                                                                   (ancestors-map %)
                                                                   %) a))))))
         (remove is-field-projection))))

(defn commands->flow
  "Transforms a series of commands into a Cascading flow"
  [commands]
  (let [flowdef (reduce (fn [def cmd] (command->flowdef cmd def)) {} (preprocess-commands commands))
        {:keys [sources pipe-to-sink sinks pipes]} flowdef
        sources-map (into {} (map (fn [s] [(str s) (sources s)]) (keys sources)))
        sinks-map (into {} (map (fn [[p s]] [(str p) (sinks s)]) pipe-to-sink))
        tail-pipes (into-array Pipe (map #(pipes %) (keys pipe-to-sink)))]
    (println "\n\nflowdef" flowdef)
    (println "sources-map" sources-map)
    (println "sinks-map" sinks-map)
    (println "tail-pipes" (map identity tail-pipes))
    (.connect (HadoopFlowConnector.) sources-map sinks-map tail-pipes)))

(defn generate-flow
  "Transforms the relation specified into a Cascading flow that is ready to be executed."
  ([query] (generate-flow {} query))
  ([opts query]
   (->> query
        (oven/bake :cascading opts)
        commands->flow)))
