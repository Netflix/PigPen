(ns pigpen.tutorial-test)

(.mkdirs (java.io.File. "build/tutorial-test"))
(ns pigpen-demo.core)

(require '[pigpen.core :as pig])

(defn my-data []
  (pig/load-tsv "build/tutorial-test/input.tsv"))

(spit "build/tutorial-test/input.tsv" "1\t2\tfoo\n4\t5\tbar")

(pig/dump (my-data))

(defn my-data []
  (->>
    (pig/load-tsv "input.tsv")
    (pig/map (fn [[a b c]]
               {:sum (+ (Integer/valueOf a) (Integer/valueOf b))
                :name c}))))

(pig/dump (my-data))

(defn my-data []
  (->>
    (pig/load-tsv "input.tsv")
    (pig/map (fn [[a b c]]
               {:sum (+ (Integer/valueOf a) (Integer/valueOf b))
                :name c}))
    (pig/filter (fn [{:keys [sum]}]
                  (< sum 5)))))

(pig/dump (my-data))

(defn my-data [input-file]
  (pig/load-tsv input-file))

(defn my-func [data]
  (->> data
    (pig/map (fn [[a b c]]
               {:sum (+ (Integer/valueOf a) (Integer/valueOf b))
                :name c}))
    (pig/filter (fn [{:keys [sum]}]
                  (< sum 5)))))

(defn my-query [input-file output-file]
  (->>
    (my-data input-file)
    (my-func)
    (pig/store-clj output-file)))

(use 'clojure.test)

(deftest test-my-func
  (let [data (pig/return [["1" "2" "foo"] ["4" "5" "bar"]])]
    (is (= (pig/dump (my-func data))
           [{:sum 3, :name "foo"}]))))

(pig/write-script "build/tutorial-test/my-script.pig" (my-query "input.tsv" "output.clj"))

(deftest test-join
  (let [left  (pig/return [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
        right (pig/return [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])

        command (pig/join (left on :a)
                          (right on :c)
                          (fn [l r] [(:b l) (:d r)]))]

    (is (= (pig/dump command)
           [[2 "foo"]
            [3 "foo"]
            [4 "bar"]
            [4 "baz"]]))))

(deftest test-cogroup
  (let [left  (pig/return [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
        right (pig/return [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])

        command (pig/cogroup (left on :a)
                             (right on :c)
                             (fn [k l r] [k (map :b l) (map :d r)]))]

    (is (= (pig/dump command)
           [[1 [2 3] ["foo"]]
            [2 [4]   ["bar" "baz"]]]))))
