(defproject pigpen "0.1.0"
  :description "PigPen - Map-Reduce for Clojure"
  :url "https://stash.corp.netflix.com/projects/SOCIAL/repos/pigpen/browse"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.2"]
                 [clj-time "0.5.0"]
                 [clj-http "0.7.6"]
                 [instaparse "1.0.1"]
                 [com.taoensso/nippy "2.0.0-RC1"]
                 [rhizome "0.1.9"]
                 [com.netflix.rxjava/rxjava-core "0.9.2"]
                 [com.netflix.rxjava/rxjava-clojure "0.9.2"]
                 [org.apache.pig/pig "0.11.1"]
                 [org.apache.hadoop/hadoop-core "1.1.2"]]
  :profiles {:dev {:dependencies
                   [[org.apache.pig/pig "0.11.1"]
                    [org.apache.hadoop/hadoop-core "1.1.2"]]}}
  :java-source-paths ["src"]
  :warn-on-reflection true
  :plugins [[codox "0.6.6"]]
  :codox {:include [pigpen.core]
          :src-dir-uri "https://stash.corp.netflix.com/projects/SOCIAL/repos/pigpen/browse/"
          :src-linenum-anchor-prefix "L"})
