(ns pigpen.cascading
  (:import (cascading.tap.hadoop Hfs)
           (cascading.scheme.hadoop TextLine)
           (cascading.pipe Pipe Each CoGroup Every)
           (cascading.flow.hadoop HadoopFlowConnector)
           (pigpen.cascading PigPenFunction PigPenBuffer)
           (cascading.tuple Fields)
           (cascading.operation Identity)
           (cascading.pipe.joiner OuterJoin))
  (:require [pigpen.raw :as raw]
            [pigpen.oven :as oven]))

;; TODO: there must be a better way to pass a Tap to a storage definition.
(defn- get-tap-fn [name]
  (let [tap ({"text" (fn [location] (Hfs. (TextLine.) location))}
             name)]
    (if (nil? tap)
      (throw (Exception. (str "Unrecognized tap type: " name)))
      tap)))

(defn load-text [location]
  (-> (raw/load$ location '[offset line] (raw/storage$ [] "text" {}) {})))

(defn load-tsv
  ([location] (load-tsv location "\t"))
  ([location delimiter]
   (-> (load-text location)
       (raw/bind$
         `(pigpen.pig/map->bind (fn [~'offset ~'line] (pigpen.extensions.core/structured-split ~'line ~delimiter)))
         {:args          '[offset line]
          :field-type-in :native}))))

(defn load-clj [location]
  (-> (load-text location)
      (raw/bind$
        `(pigpen.pig/map->bind (fn [~'offset ~'line] (clojure.edn/read-string ~'line)))
        {:args          '[offset line]
         :field-type-in :native})))

(defn store-text [location f relation]
  (-> relation
      (raw/bind$ `(pigpen.pig/map->bind ~f)
                 {:args (:fields relation), :field-type-out :native})
      (raw/store$ location (raw/storage$ [] "text" {}) {})))

(defn store-tsv
  ([location relation] (store-tsv location "\t" relation))
  ([location delimiter relation]
   (store-text location `(fn [~'s] (clojure.string/join ~delimiter (map print-str ~'s))) relation)))

(defn store-clj [location relation]
  (store-text location `clojure.core/pr-str relation))

(defmulti command->flowdef
          "Converts an individual command into the equivalent Cascading flow definition."
          (fn [{:keys [type]} flowdef] type))

(defmethod command->flowdef :register
           [command flowdef]
  flowdef)

(defmethod command->flowdef :load
           [{:keys [id location storage fields opts]} flowdef]
  {:pre [id location storage fields]}
  (update-in flowdef [:sources] (partial merge {id ((get-tap-fn (:func storage)) location)})))

(defmethod command->flowdef :store
           [{:keys [id ancestors location storage opts]} flowdef]
  {:pre [id ancestors location storage]}
  (-> flowdef
      (update-in [:pipe-to-sink] (partial merge {(first ancestors) id}))
      (update-in [:sinks] (partial merge {id ((get-tap-fn (:func storage)) location)}))))

(defn- cascading-field [name-or-number]
  (if (number? name-or-number)
    (int name-or-number)
    (str name-or-number)))

(defmethod command->flowdef :code
           [{:keys [return expr args pipe field-projections]} flowdef]
  {:pre [return expr args]}
  (let [{:keys [init func]} expr
        fields (if field-projections
                 (Fields. (into-array (map #(cascading-field (:alias %)) field-projections)))
                 Fields/UNKNOWN)
        is-group #(cond (instance? CoGroup %) true
                        (nil? %) false
                        :else (recur (first (.getPrevious %))))]
    (if (is-group (get-in flowdef [:pipes pipe]))
      (update-in flowdef [:pipes pipe] #(Every. % (PigPenBuffer. (str init) (str func) fields) Fields/RESULTS))
      (update-in flowdef [:pipes pipe] #(Each. % (PigPenFunction. (str init) (str func) fields) Fields/RESULTS)))))

(defn- get-cogroup-fields
  "Pig does something like [k, v1], [k, v2] > [group, v1, v2], while cascading
  does [k, v1], [k, v2] > [k, v1, k_copy, v2], where the number of fields of
  the combined stream must match the sum of the streams being joined, but the field
  names are arbitrary."
  [fields]
  (cons (first fields) (nthrest fields 2)))

(defmethod command->flowdef :group
           [{:keys [id keys fields join-types ancestors opts]} flowdef]
  {:pre [id keys fields join-types ancestors]}
  (println "flowdef" flowdef)
  (println "keys" keys)
  (println "fields" fields)
  (println "ancestors" ancestors)
  (println "join-types" join-types)
  (update-in flowdef [:pipes] (partial merge {id (CoGroup. (str id)
                                                           (into-array Pipe (map (:pipes flowdef) ancestors))
                                                           (into-array (map #(Fields. (into-array (map str %))) keys))
                                                           (Fields. (into-array (map str (get-cogroup-fields fields)))))})))

(defmethod command->flowdef :projection-flat
           [{:keys [code alias pipe field-projections]} flowdef]
  {:pre [code alias]}
  (command->flowdef (assoc code :pipe pipe :field-projections field-projections) flowdef))

(defmethod command->flowdef :generate
           [{:keys [id ancestors projections field-projections opts]} flowdef]
  {:pre [id ancestors (not-empty projections)]}
  (let [new-flowdef (reduce (fn [def ancestor]
                              (cond (contains? (:sources def) ancestor) (let [pipe (Pipe. (str id))]
                                                                          (-> def
                                                                              (update-in [:pipe-to-source] (partial merge {id ancestor}))
                                                                              (update-in [:pipes] (partial merge {id pipe}))))
                                    (contains? (:pipes def) ancestor) (let [pipe ((:pipes def) ancestor)]
                                                                        (-> def
                                                                            (update-in [:pipes] (partial merge {id (Pipe. (str id) pipe)}))))
                                    :else (do
                                            (println "flowdef" flowdef)
                                            (println "id" id)
                                            (println "ancestors" ancestors)
                                            (throw (Exception. "not implemented")))))
                            flowdef ancestors)
        flat-projections (filter #(= :projection-flat (:type %)) projections)
        new-flowdef (reduce (fn [def cmd] (command->flowdef (assoc cmd :pipe id :field-projections field-projections) def))
                            new-flowdef
                            flat-projections)]
    new-flowdef))

(defmethod command->flowdef :default
           [command flowdef]
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
        {:keys [pipe-to-source sources pipe-to-sink sinks pipes]} flowdef
        sources-map (into {} (map (fn [[p s]] [(str p) (sources s)]) pipe-to-source))
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
        (oven/bake opts)
        commands->flow)))
