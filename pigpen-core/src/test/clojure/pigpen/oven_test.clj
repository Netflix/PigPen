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

(ns pigpen.oven-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [schema.core :as s]
            [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.model :as m]
            [pigpen.oven :refer :all]
            [pigpen.raw :as pig-raw]
            [pigpen.io :as pig-io]
            [pigpen.map :as pig-map]
            [pigpen.filter :as pig-filter]
            [pigpen.join :as pig-join]))

(use-fixtures :once schema.test/validate-schemas)

(deftest test-tree->command
  (s/validate m/Store
              (->>
                (pig-raw/load$ "foo" :string '[foo] {})
                (pig-raw/store$ "bar" :string {})
                (#'pigpen.oven/tree->command))))

(deftest test-braise
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (test-diff
      (->>
        (pig-raw/load$ "foo" :string '[foo] {})
        (pig-raw/store$ "bar" :string {})
        (#'pigpen.oven/braise {})
        (map #(select-keys % [:type :id :ancestors :fields])))
      '[{:type :load
         :id load1
         :fields [load1/foo]}
        {:type :store
         :id store2
         :ancestors (load1)}])))

(deftest test-next-match
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [s1 (->>
               (pig-raw/load$ "foo" :string '[foo] {})
               (pig-raw/store$ "bar" :string {}))

          s2 (->>
               (pig-raw/load$ "foo" :string '[foo] {})
               (pig-raw/store$ "bar2" :string {}))]

      (test-diff
        (->> (pig-io/store-many s1 s2)
          (#'pigpen.oven/braise {})
          (#'pigpen.oven/next-match))
        ;; Should match the load commands
        '{load1 load3}))))

(deftest test-merge-command
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [l1 (pig-raw/load$ "foo" :string '[foo] {})
          s1 (pig-raw/store$ "bar" :string {} l1)
          l2 (pig-raw/load$ "foo" :string '[foo] {})
          s2 (pig-raw/store$ "bar2" :string {} l2)
          s (pig-io/store-many s1 s2)

          mapping {(:id l1) (:id l2)}
          commands (#'pigpen.oven/braise {} s)]

      (test-diff
        (->> commands
          (#'pigpen.oven/merge-command mapping)
          (map #(select-keys % [:type :id :ancestors])))
        ;; Should make the ids of the load commands the same
        '[{:type :load,       :id load3}
          {:type :store,      :id store4,      :ancestors [load3]}
          {:type :load,       :id load3}
          {:type :store,      :id store2,      :ancestors [load3]}
          {:type :store-many, :id store-many5, :ancestors [store2 store4]}])))

  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [r ^:pig {:id 'r :fields '[r/value], :field-type :frozen}
          s ^:pig {:id 's :fields '[s/value], :field-type :frozen}
          c (pig-join/join [(r :on identity)
                            (s :on identity)]
                           merge)
          mapping '{r r0, s s0, bind1 b1, bind2 b2, join3 j3, bind4 b4}
          commands (#'pigpen.oven/braise {} c)]

      (test-diff
        (->> commands
          (#'pigpen.oven/merge-command mapping)
          (map #(select-keys % [:type :id :ancestors :keys :fields :args])))
        '[{:id s0
           :fields [s0/value]}
          {:type :bind
           :id b2
           :ancestors [s0]
           :fields [b2/key b2/value]
           :args [s0/value]}
          {:id r0
           :fields [r0/value]}
          {:type :bind
           :id b1
           :ancestors [r0]
           :fields [b1/key b1/value]
           :args [r0/value]}
          {:type :join
           :id j3
           :ancestors [b1 b2]
           :keys [b1/key b2/key]
           :fields [b1/key b1/value b2/key b2/value]}
          {:type :bind
           :id b4
           :ancestors [j3]
           :fields [b4/value]
           :args [b1/value b2/value]}]))))

(deftest test-dedupe
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s1 (->>
               (pig-raw/load$ "foo" :string '[foo] {})
               (pig-raw/store$ "bar" :string {}))

          s2 (->>
               (pig-raw/load$ "foo" :string '[foo] {})
               (pig-raw/store$ "bar2" :string {}))]

      (test-diff
        (->> (pig-io/store-many s1 s2)
          (#'pigpen.oven/braise {})
          (#'pigpen.oven/dedupe {})
          (map #(select-keys % [:type :id :ancestors])))
        ;; Should merge the load commands
        '[{:type :load,       :id load3}
          {:type :store,      :id store4,      :ancestors [load3]}
          {:type :store,      :id store2,      :ancestors [load3]}
          {:type :store-many, :id store-many5, :ancestors [store2 store4]}]))))

(deftest test-debug
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [script (->>
                   (pig-raw/load$ "foo" :string '[foo] {})
                   (pig-raw/project$ [(pig-raw/projection-field$ 'load1/foo ['bar])] {})
                   (pig-raw/store$ "bar" :string {})
                   (vector)
                   (pig-raw/store-many$))]

      (test-diff
        (->> script
          (bake :pig {} {:debug "/out/"})
          (map #(select-keys % [:type :id :ancestors :location])))
        '[{:type :load,       :id load1,                               :location "foo"}
          {:type :project,    :id project10,   :ancestors [load1]}
          {:type :store,      :id store9,      :ancestors [project10], :location "/out/load1"}
          {:type :project,    :id project2,    :ancestors [load1]}
          {:type :project,    :id project11,   :ancestors [project2]}
          {:type :store,      :id store7,      :ancestors [project11], :location "/out/project2"}
          {:type :store,      :id store3,      :ancestors [project2],  :location "bar"}
          {:type :store-many, :id store-many4, :ancestors [store3]}
          {:type :store-many, :id store-many5, :ancestors [store-many4 store7 store9]}]))))

(defn clean-bind-sequence [commands]
  (mapv #(mapv (juxt :id :description) %) commands))

(let [^:local s0 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                   (->>
                     (pig-io/load-clj "in")
                     (pig-map/map identity)
                     (pig-io/store-clj "out")
                     (#'pigpen.oven/braise {})
                     (#'pigpen.oven/dedupe {})))

      ^:local s1 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                   (->>
                     (pig-io/load-clj "in")
                     (pig-map/map identity)
                     (pig-filter/filter (constantly true))
                     (pig-map/mapcat vector)
                     (pig-io/store-clj "out")
                     (#'pigpen.oven/braise {})
                     (#'pigpen.oven/dedupe {})))

      ^:local s2 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                   (let [p0 (->>
                              (pig-io/load-clj "in")
                              (pig-map/map identity)
                              (pig-map/map identity))

                         p1 (->> p0
                              (pig-filter/filter (constantly true))
                              (pig-map/map inc)
                              (pig-io/store-clj "out0"))

                         p2 (->> p0
                              (pig-map/map dec)
                              (pig-filter/filter (constantly false))
                              (pig-io/store-clj "out1"))]
                     (->>
                       (pig-io/store-many p1 p2)
                       (#'pigpen.oven/braise {})
                       (#'pigpen.oven/dedupe {}))))

      ^:local s3 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                   (let [p0 (->>
                              (pig-io/load-clj "in0")
                              (pig-map/map inc))

                         p1 (->>
                              (pig-io/load-clj "in1")
                              (pig-map/map dec))]
                     (->>
                       (pig-join/join [(p0 :on identity)
                                       (p1 :on identity)]
                                      merge)
                       (pig-filter/filter (constantly true))
                       (pig-io/store-clj "out")
                       (#'pigpen.oven/braise {})
                       (#'pigpen.oven/dedupe {}))))]

  (deftest test-find-bind-sequence
    (testing "s0"
      (test-diff (clean-bind-sequence
                   (#'pigpen.oven/find-bind-sequence s0))
                 '[[[load1 "in"]]
                   [[bind2 nil] [bind3 "identity\n"] [bind4 nil]]
                   [[store5 "out"]]]))

    (testing "s1"
      (test-diff (clean-bind-sequence
                   (#'pigpen.oven/find-bind-sequence s1))
                 '[[[load1 "in"]]
                   [[bind2 nil] [bind3 "identity\n"] [bind4 "(constantly true)\n"] [bind5 "vector\n"] [bind6 nil]]
                   [[store7 "out"]]]))

    (testing "s2"
      (test-diff (clean-bind-sequence
                   (#'pigpen.oven/find-bind-sequence s2))
                 '[[[load1 "in"]]
                   [[bind2 nil] [bind3 "identity\n"] [bind4 "identity\n"]]
                   [[bind9 "dec\n"] [bind10 "(constantly false)\n"] [bind11 nil] [store12 "out1"] [bind5 "(constantly true)\n"] [bind6 "inc\n"] [bind7 nil] [store8 "out0"] [store-many13 nil]]]))

    (testing "s3"
      (test-diff (clean-bind-sequence
                   (#'pigpen.oven/find-bind-sequence s3))
                 '[[[load4 "in1"]]
                   [[bind5 nil] [bind6 "dec\n"] [bind8 nil]]
                   [[load1 "in0"] [bind2 nil] [bind3 "inc\n"] [bind7 nil] [join9 "merge\n"] [bind10 nil] [bind11 "(constantly true)\n"] [bind12 nil] [store13 "out"]]])))

  (deftest test-bind->project

    (testing "s1"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (let [[_ binds _] (#'pigpen.oven/find-bind-sequence s1)]
          (test-diff (#'pigpen.oven/bind->project binds :pig)
                     '{:fields [project1/value]
                       :ancestors [load1]
                       :projections [{:type :projection
                                      :expr {:type :code
                                             :init (clojure.core/require (quote [pigpen.runtime]) (quote [clojure.edn]))
                                             :func (clojure.core/comp
                                                     (pigpen.runtime/process->bind (pigpen.runtime/pre-process :pig :native))
                                                     (pigpen.runtime/map->bind clojure.edn/read-string)
                                                     (pigpen.runtime/map->bind (pigpen.runtime/with-ns pigpen.oven-test identity))
                                                     (pigpen.runtime/filter->bind (pigpen.runtime/with-ns pigpen.oven-test (constantly true)))
                                                     (pigpen.runtime/mapcat->bind (pigpen.runtime/with-ns pigpen.oven-test vector))
                                                     (pigpen.runtime/map->bind clojure.core/pr-str)
                                                     (pigpen.runtime/process->bind (pigpen.runtime/post-process :pig :native)))
                                             :udf :seq
                                             :args [load1/value]}
                                      :flatten true
                                      :alias [project1/value]
                                      :types nil}]
                       :type :project
                       :id project1
                       :description "identity\n(constantly true)\nvector\n"
                       :field-type :native
                       :opts {:type :project-opts}})))))

  (deftest test-optimize-binds

    (testing "s0"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds {:platform :pig} s0))
                   '[[load1 "in"]
                     [project1 "identity\n"]
                     [store5 "out"]])))

    (testing "s1"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds {:platform :pig} s1))
                   '[[load1 "in"]
                     [project1 "identity\n(constantly true)\nvector\n"]
                     [store7 "out"]])))

    (testing "s2"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds {:platform :pig} s2))
                   '[[load1 "in"]
                     [project1 "identity\nidentity\n"]
                     [project2 "dec\n(constantly false)\n"]
                     [store12 "out1"]
                     [project3 "(constantly true)\ninc\n"]
                     [store8 "out0"]
                     [store-many13 nil]])))

    (testing "s3"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds {:platform :pig} s3))
                   '[[load4 "in1"]
                     [project1 "dec\n"]
                     [load1 "in0"]
                     [project2 "inc\n"]
                     [join9 "merge\n"]
                     [project3 "(constantly true)\n"]
                     [store13 "out"]])))))

(deftest test-alias-self-joins
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [data (pig-io/return [1 2 3])
          command (pig-join/join [(data) (data)] vector)]
      (test-diff (->> command
                   (bake :pig {} {})
                   (map #(select-keys % [:fields :ancestors :id :type])))
                 '[{:type :return,  :id return1,                            :fields [return1/value]}
                   {:type :project, :id project6, :ancestors [return1],     :fields [project6/key project6/value]}
                   {:type :noop,    :id noop8,    :ancestors [project6],    :fields [noop8/key noop8/value]}
                   {:type :noop,    :id noop9,    :ancestors [project6],    :fields [noop9/key noop9/value]}
                   {:type :join,    :id join4,    :ancestors [noop8 noop9], :fields [noop8/key noop8/value noop9/key noop9/value]}
                   {:type :project, :id project7, :ancestors [join4],       :fields [project7/value]}]))))

(deftest test-bake
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s1 (->>
               (pig-raw/load$ "foo" :test-storage '[foo] {})
               (pig-raw/store$ "bar" :test-storage {}))

          s2 (->>
               (pig-raw/load$ "foo" :test-storage '[foo] {})
               (pig-raw/store$ "bar2" :test-storage {}))]

      (test-diff
        (->>
          (pig-io/store-many s1 s2)
          (#'pigpen.oven/bake :pig {} {})
          (map #(select-keys % [:type :id :ancestors])))
        '[{:type :load,       :id load3}
          {:type :store,      :id store4,      :ancestors [load3]}
          {:type :store,      :id store2,      :ancestors [load3]}
          {:type :store-many, :id store-many5, :ancestors [store2 store4]}]))))
