;;
;;
;;  Copyright 2013 Netflix, Inc.
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
  (:use clojure.test
        pigpen.oven)
  (:require [pigpen.extensions.test :refer [test-diff pigsym-zero pigsym-inc]]
            [pigpen.raw :as pig-raw]
            [pigpen.core :as pig]))

(deftest test-tree->command
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (test-diff
      (->
        (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
        (pig-raw/store$ "bar" pig-raw/default-storage {})
        (#'pigpen.oven/tree->command))
      '{:type :store
        :id store2
        :description "bar"
        :ancestors (load1)
        :location "bar"
        :storage {:type :storage, :references [], :func "PigStorage", :args []}
        :fields [foo]
        :opts {:type :store-opts}})))

(deftest test-command->required-fields
  (is (= '#{foo bar}
         (#'pigpen.oven/command->required-fields
           '{:type :generate
             :id generate0
             :projections [{:type :projection-field, :field foo, :alias baz}
                           {:type :projection-func
                            :code {:type :code, :udf "f", :args [foo bar]}
                            :alias bar}]
             :fields [baz bar]
             :ancestors [{:fields [foo bar]}]})))
  
  (is (= nil
         (#'pigpen.oven/command->required-fields
           (pig-raw/load$ "foo" '[a0 b0 c0 d0] pig-raw/default-storage {})))))

(deftest test-remove-fields
  
  (let [command '{:type :generate
                  :id generate0
                  :projections [{:type :projection-field, :field foo, :alias baz}
                                {:type :projection-func
                                 :code {:type :code, :udf "f", :args [foo bar]}
                                 :alias bar}]
                  :fields [baz bar]
                  :ancestors [{:fields [foo bar]}]}]
    
    (test-diff
      (#'pigpen.oven/remove-fields command '#{baz})
      '{:type :generate
        :id generate0
        :projections [{:type :projection-func
                       :code {:type :code, :udf "f", :args [foo bar]}
                       :alias bar}]
        :fields [bar]
        :ancestors [{:fields [foo bar]}]})
      
    (test-diff
      (#'pigpen.oven/remove-fields command '#{bar})
      '{:type :generate
        :id generate0
        :projections [{:type :projection-field, :field foo, :alias baz}]
        :fields [baz]
        :ancestors [{:fields [foo bar]}]})))

(deftest test-command->references
  
  (test-diff
    (#'pigpen.oven/command->references "s3://bucket/pigpen.jar"
                                       (pig-raw/generate$ {}
                                         [(pig-raw/projection-func$ 'foo
                                            (pig-raw/code$ :normal ['foo]
                                              (pig-raw/expr$ '(require '[pigpen.pig])
                                                             '(var clojure.core/prn))))] {}))
    ["s3://bucket/pigpen.jar"])
  
  (test-diff
    (#'pigpen.oven/command->references "pigpen.jar"
                                       (pig-raw/store$ {} "" (pig-raw/storage$ ["my-jar.jar"] "f" []) {}))
    ["my-jar.jar"]))

(deftest test-braise
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (test-diff
      (as-> nil %
        (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
        (pig-raw/store$ % "bar" pig-raw/default-storage {})
        (#'pigpen.oven/braise %)
        (map #(select-keys % [:type :id :ancestors :fields]) %))
      '[{:ancestors []
         :type :load
         :id load1
         :fields [foo]}
        {:type :store
         :id store2
         :ancestors (load1)
         :fields [foo]}])))

(deftest test-extract-references
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [test-storage (pig-raw/storage$ ["ref"] "TestStorage" [])]
      (test-diff
        (as-> nil %
          (pig-raw/load$ "foo" '[foo] test-storage {})
          (pig-raw/store$ % "bar" test-storage {})
          (#'pigpen.oven/braise %)
          (#'pigpen.oven/extract-references "pigpen.jar" %)
          (map #(select-keys % [:type :id :ancestors :references :jar]) %))
        '[{:type :register
           :jar "ref"}
          {:ancestors []
           :type :load
           :id load1}
          {:type :store
           :id store2
           :ancestors [load1]}]))))

(deftest test-extract-options
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (test-diff
      (as-> nil %
        (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {:pig-options {"pig.maxCombinedSplitSize" 1000000}})
        (pig-raw/store$ % "bar" pig-raw/default-storage {})
        (#'pigpen.oven/braise %)
        (#'pigpen.oven/extract-options %)
        (map #(select-keys % [:type :id :ancestors :references :option :value]) %))
      '[{:type :option
         :option "pig.maxCombinedSplitSize"
         :value 1000000}
        {:type :load
         :id load1
         :ancestors []}
        {:type :store
         :id store2
         :ancestors [load1]}])))

(deftest test-next-match
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
  
    (let [s1 (->
               (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
               (pig-raw/store$ "bar" pig-raw/default-storage {}))
          
          s2 (->
               (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
               (pig-raw/store$ "bar2" pig-raw/default-storage {}))]
      
      (test-diff
        (->
          (pig/script s1 s2)
          (#'pigpen.oven/braise)
          (#'pigpen.oven/next-match))
        ;; Should match the load commands
        '{load1 load3}))))

(deftest test-merge-command
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [l1 (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
          s1 (pig-raw/store$ l1 "bar" pig-raw/default-storage {})
          l2 (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
          s2 (pig-raw/store$ l2 "bar2" pig-raw/default-storage {})
          s (pig/script s1 s2)
          
          mapping {(:id l1) (:id l2)}
          commands (#'pigpen.oven/braise s)]
      
      (test-diff
        (as-> commands %
          (#'pigpen.oven/merge-command % mapping)
          (map #(select-keys % [:type :id :ancestors]) %))
        ;; Should make the ids of the load commands the same
        '[{:type :load,   :id load3,   :ancestors []}
          {:type :store,  :id store4,  :ancestors [load3]}
          {:type :load,   :id load3,   :ancestors []}
          {:type :store,  :id store2,  :ancestors [load3]}
          {:type :script, :id script5, :ancestors [store2 store4]}])))
  
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    
    (let [r ^:pig {:id 'r :fields '[value]}
          s ^:pig {:id 's :fields '[value]}
          c (pig/join [(r :on identity)
                       (s :on identity)]
                      merge)
          mapping '{r r0, s s0, generate1 g1, generate2 g2, join3 j3, generate4 g4}
          commands (#'pigpen.oven/braise c)]
      
      (test-diff
        (as-> commands %
          (#'pigpen.oven/merge-command % mapping)
          (map #(select-keys % [:type :id :ancestors :keys :fields :projections :args]) %))
        '[{:id s0
           :ancestors []
           :fields [value]
           :args []}
          {:type :bind
           :id bind3
           :ancestors [s0]
           :fields [value]
           :args [value]}
          {:type :generate
           :id g4
           :ancestors [bind3]
           :fields [key value]
           :projections [{:type :projection-field, :field 0, :alias key}
                         {:type :projection-field, :field 1, :alias value}]
           :args []}
          {:id r0
           :ancestors []
           :fields [value]
           :args []}
          {:type :bind
           :id bind1
           :ancestors [r0]
           :fields [value]
           :args [value]}
          {:type :generate
           :id g2
           :ancestors [bind1]
           :fields [key value]
           :projections [{:type :projection-field, :field 0, :alias key}
                         {:type :projection-field, :field 1, :alias value}]
           :args []}
          {:type :join
           :id join5
           :ancestors [g2 g4]
           :keys [[key] [key]]
           :fields [[[g2 key]] [[g2 value]] [[g4 key]] [[g4 value]]]
           :args []}
          {:type :bind
           :id bind6
           :ancestors [join5]
           :fields [value]
           :args [[[g2 value]] [[g4 value]]]}]))))

(deftest test-dedupe
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
  
    (let [s1 (->
               (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
               (pig-raw/store$ "bar" pig-raw/default-storage {}))
          
          s2 (->
               (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
               (pig-raw/store$ "bar2" pig-raw/default-storage {}))]
      
      (test-diff
        (as-> (pig/script s1 s2) %
          (#'pigpen.oven/braise %)
          (#'pigpen.oven/dedupe %)
          (map #(select-keys % [:type :id :ancestors]) %))
        ;; Should merge the load commands
        '[{:type :load,   :id load3,   :ancestors []}
          {:type :store,  :id store4,  :ancestors [load3]}
          {:type :store,  :id store2,  :ancestors [load3]}
          {:type :script, :id script5, :ancestors [store2 store4]}]))))

(deftest test-merge-order-rank
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig/return ["b" "c" "a"])
              (pig/sort)
              (pig/map-indexed vector))]
      
      (test-diff
        (->> s
          (#'pigpen.oven/braise)
          (#'pigpen.oven/merge-order-rank)
          (map #(select-keys % [:type :id :ancestors :sort-keys])))
        '[{:type :return,   :id return1,   :ancestors []}
          {:type :bind,     :id bind2,     :ancestors [return1]}
          {:type :generate, :id generate3, :ancestors [bind2]}
          {:type :order,    :id order4,    :ancestors [generate3], :sort-keys [key :asc]}
          {:type :rank,     :id rank5,     :ancestors [generate3], :sort-keys [key :asc]}
          {:type :bind,     :id bind6,     :ancestors [rank5]}]))))


;; Column pruning really doesn't matter anymore. Considering removing it.
#_(deftest test-command->fat
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [commands (->
                     {:id 'r :fields '[foo bar baz]}
                     (pig/generate [foo0 foo
                                    bar0 bar])
                     (#'pigpen.oven/braise))]
    
      (is (= '#{baz}
             (#'pigpen.oven/command->fat commands (first commands)))))
    
    (let [commands (->
                     {:id 'r :fields '[foo bar baz]}
                     (pig/generate [foo0 (str foo)
                                    bar0 bar])
                     (#'pigpen.oven/braise))]
    
      (is (= '#{}
             (#'pigpen.oven/command->fat commands (first commands)))))
    
    (let [commands (->
                     {:id 'r :fields '[foo bar baz]}
                     (pig/group [foo])
                     (#'pigpen.oven/braise))]
    
      (is (= nil
             (#'pigpen.oven/command->fat commands (first commands)))))
    
    (let [commands (->
                     (pig-raw/load$ "foo" '[a0 b0 c0 d0] pig-raw/default-storage {})
                     (#'pigpen.oven/braise))]
    
      (is (= nil
             (#'pigpen.oven/command->fat commands (first commands)))))))

#_(deftest test-trim-fat
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [commands (->
                     (pig-raw/load$ "foo" '[a0 b0 c0 d0] pig-raw/default-storage {})
                     (pig/generate [a1 a0, b1 b0, c1 c0])
                     (pig/generate [a2 a1, b2 b1])
                     (pig/generate [a3 a2])
                     (#'pigpen.oven/braise)
                     (#'pigpen.oven/trim-fat))]
      
      (test-diff
        commands
        '[{:ancestors []
           :type :load
           :id load1
           :location "foo"
           :fields [a0 b0 c0 d0]
           :storage {:type :storage, :references [], :func "PigStorage", :args []}
           :references []
           :opts {}}
          {:type :generate
           :id generate2
           :projections [{:type :projection-field, :field a0, :alias a1}]
           :fields [a1]
           :references []
           :ancestors [load1]}
          {:type :generate
           :id generate3
           :projections [{:type :projection-field, :field a1, :alias a2}]
           :fields [a2]
           :references []
           :ancestors [generate2]}
          {:type :generate
           :id generate4
           :projections [{:type :projection-field, :field a2, :alias a3}]
           :fields [a3]
           :references []
           :ancestors [generate3]}]))))

(deftest test-debug
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
  
    (let [script (->
                   (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {})
                   (pig-raw/generate$ [(pig-raw/projection-field$ 'foo 'bar)] {})
                   (pig-raw/store$ "bar" pig-raw/default-storage {})
                   (vector)
                   (pig-raw/script$))]
      
      (test-diff
        (->> script
          (bake :pig {:debug "/out/"})
          (map #(select-keys % [:type :id :ancestors :location])))
        '[{:type :register, :id nil,       :ancestors []}
          {:type :load,     :id load1,     :ancestors []            :location "foo"}
          {:type :generate, :id generate10,:ancestors [load1]}
          {:type :store,    :id store9,    :ancestors [generate10], :location "/out/load1"}
          {:type :generate, :id generate2, :ancestors [load1]}
          {:type :generate, :id generate11,:ancestors [generate2]}
          {:type :store,    :id store7,    :ancestors [generate11], :location "/out/generate2"}
          {:type :store,    :id store3,    :ancestors [generate2],  :location "bar"}
          {:type :script,   :id script4,   :ancestors [store3]}
          {:type :script,   :id script5,   :ancestors [script4 store7 store9]}]))))

(defn clean-bind-sequence [commands]
  (mapv #(mapv (juxt :id :description) %) commands))

(let [^:local s0 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                   (->>
                     (pig/load-clj "in")
                     (pig/map identity)
                     (pig/store-clj "out")
                     (#'pigpen.oven/braise)
                     (#'pigpen.oven/dedupe)))
      
      ^:local s1 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                  (->>
                    (pig/load-clj "in")
                    (pig/map identity)
                    (pig/filter (constantly true))
                    (pig/mapcat vector)
                    (pig/store-clj "out")
                    (#'pigpen.oven/braise)
                    (#'pigpen.oven/dedupe)))
      
      ^:local s2 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                   (let [p0 (->>
                              (pig/load-clj "in")
                              (pig/map identity)
                              (pig/map identity))
                 
                         p1 (->> p0
                              (pig/filter (constantly true))
                              (pig/map inc)
                              (pig/store-clj "out0"))
                 
                         p2 (->> p0
                              (pig/map dec)
                              (pig/filter (constantly false))
                              (pig/store-clj "out1"))]
                     (->> (pig/script p1 p2)
                       (#'pigpen.oven/braise)
                       (#'pigpen.oven/dedupe))))
      
      ^:local s3 (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
                   (let [p0 (->>
                              (pig/load-clj "in0")
                              (pig/map inc))
                 
                         p1 (->>
                              (pig/load-clj "in1")
                              (pig/map dec))]
                     (->>
                       (pig/join [(p0 :on identity)
                                  (p1 :on identity)]
                                 merge)
                       (pig/filter (constantly true))
                       (pig/store-clj "out")
                       (#'pigpen.oven/braise)
                       (#'pigpen.oven/dedupe))))]
  
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
                  [[bind9 "dec\n"] [bind10 "(constantly false)\n"] [bind11 nil] [store12 "out1"] [bind5 "(constantly true)\n"] [bind6 "inc\n"] [bind7 nil] [store8 "out0"] [script13 nil]]]))
    
    (testing "s3"
       (test-diff (clean-bind-sequence
                    (#'pigpen.oven/find-bind-sequence s3))
                  '[[[load4 "in1"]]
                    [[bind5 nil] [bind6 "dec\n"] [bind9 nil]]
                    [[generate10 nil] [load1 "in0"] [bind2 nil] [bind3 "inc\n"] [bind7 nil] [generate8 nil] [join11 "merge\n"] [bind12 nil] [bind13 "(constantly true)\n"] [bind14 nil] [store15 "out"]]])))
  
  (deftest test-bind->generate
    
    (testing "s1"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (let [[_ binds _] (#'pigpen.oven/find-bind-sequence s1)]
          (test-diff (#'pigpen.oven/bind->generate binds :pig)
                     '{:fields [value]
                       :ancestors [load1]
                       :projections [{:type :projection-flat
                                      :code {:type :code
                                             :expr {:init (clojure.core/require (quote [pigpen.runtime]) (quote [clojure.edn]))
                                                    :func (pigpen.runtime/exec [(pigpen.runtime/process->bind (pigpen.runtime/pre-process :pig :native))
                                                                                (pigpen.runtime/map->bind clojure.edn/read-string)
                                                                                (pigpen.runtime/map->bind (pigpen.runtime/with-ns pigpen.oven-test identity))
                                                                                (pigpen.runtime/filter->bind (pigpen.runtime/with-ns pigpen.oven-test (constantly true)))
                                                                                (pigpen.runtime/mapcat->bind (pigpen.runtime/with-ns pigpen.oven-test vector))
                                                                                (pigpen.runtime/map->bind clojure.core/pr-str)
                                                                                (pigpen.runtime/process->bind (pigpen.runtime/post-process :pig :native))])}
                                             :udf :sequence
                                             :args [value]}
                                      :alias value}]
                       :type :generate
                       :id generate1
                       :description "identity\n(constantly true)\nvector\n"
                       :field-type :native
                       :opts {:type :generate-opts
                              :implicit-schema nil}})))))
  
  
  (deftest test-optimize-binds
    
    (testing "s0"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds s0 :pig))
                   '[[load1 "in"]
                     [generate1 "identity\n"]
                     [store5 "out"]])))
    
    (testing "s1"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds s1 :pig))
                   '[[load1 "in"]
                     [generate1 "identity\n(constantly true)\nvector\n"]
                     [store7 "out"]])))
    
    (testing "s2"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds s2 :pig))
                   '[[load1 "in"]
                     [generate1 "identity\nidentity\n"]
                     [generate2 "dec\n(constantly false)\n"]
                     [store12 "out1"]
                     [generate3 "(constantly true)\ninc\n"]
                     [store8 "out0"]
                     [script13 nil]])))
    
    (testing "s3"
      (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
        (test-diff (map (juxt :id :description) (#'pigpen.oven/optimize-binds s3 :pig))
                   '[[load4 "in1"]
                     [generate1 "dec\n"]
                     [generate10 nil]
                     [load1 "in0"]
                     [generate2 "inc\n"]
                     [generate8 nil]
                     [join11 "merge\n"]
                     [generate3 "(constantly true)\n"]
                     [store15 "out"]])))))

(deftest test-expand-load-filters
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [command (pig-raw/load$ "foo" '[foo] pig-raw/default-storage {:filter '(= foo 2)})]
      (test-diff (bake :pig command)
                 '[{:type :load
                    :id load1_0
                    :description "foo"
                    :location "foo"
                    :ancestors []
                    :fields [foo]
                    :field-type :native
                    :args []
                    :storage {:type :storage
                              :references []
                              :func "PigStorage"
                              :args []}
                    :opts {:type :load-opts
                           :filter (= foo 2)}}
                   {:type :filter-native
                    :id load1
                    :description nil
                    :ancestors [load1_0]
                    :expr (= foo 2)
                    :fields [foo]
                    :field-type :native
                    :args []
                    :opts {:type :filter-native-opts}}]))))

(deftest test-alias-self-joins
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
    (let [data (pig/return [1 2 3])
          command (pig/join [(data) (data)] vector)]
      (test-diff (->> (bake :pig command)
                   (map #(select-keys % [:fields :ancestors :id :type])))
                  '[{:type :register, :id nil,        :ancestors [],                      :fields []}
                    {:type :return,   :id return1,    :ancestors [],                      :fields [value]}
                    {:type :generate, :id generate8,  :ancestors [return1],               :fields [value]}
                    {:type :generate, :id generate10, :ancestors [generate8],             :fields [key value]}
                    {:type :generate, :id generate11, :ancestors [generate8],             :fields [key value]}
                    {:type :join,     :id join6,      :ancestors [generate10 generate11], :fields [[[generate10 key]] [[generate10 value]] [[generate11 key]] [[generate11 value]]]}
                    {:type :generate, :id generate9,  :ancestors [join6],                 :fields [value]}]))))

(deftest test-clean
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]

    (let [s (->> (pig/return ["b" "c" "a"])
              (pig/sort)
              (pig/map-indexed vector))]
      
      (test-diff
        (->> s
          (#'pigpen.oven/braise)
          (#'pigpen.oven/merge-order-rank)
          (#'pigpen.oven/clean)
          (map #(select-keys % [:type :id :ancestors :sort-keys])))
        '[{:type :return,   :id return1,   :ancestors []}
          {:type :bind,     :id bind2,     :ancestors [return1]}
          {:type :generate, :id generate3, :ancestors [bind2]}
          {:type :rank,     :id rank5,     :ancestors [generate3], :sort-keys [key :asc]}
          {:type :bind,     :id bind6,     :ancestors [rank5]}]))))

(deftest test-bake
  (with-redefs [pigpen.raw/pigsym (pigsym-inc)]
  
    (let [test-storage (pig-raw/storage$ ["ref"] "TestStorage" [])
          
          s1 (->
               (pig-raw/load$ "foo" '[foo] test-storage {})
               (pig-raw/store$ "bar" test-storage {}))
          
          s2 (->
               (pig-raw/load$ "foo" '[foo] test-storage {})
               (pig-raw/store$ "bar2" test-storage {}))]

      (test-diff
        (->> (pig/script s1 s2)
          (#'pigpen.oven/bake :pig)
          (map #(select-keys % [:type :id :ancestors])))
        ;; Should merge the load commands & produce a register command
        '[{:type :register, :id nil,   :ancestors []}
          {:type :load,   :id load3,   :ancestors []}
          {:type :store,  :id store4,  :ancestors [load3]}
          {:type :store,  :id store2,  :ancestors [load3]}
          {:type :script, :id script5, :ancestors [store2 store4]}]))))
