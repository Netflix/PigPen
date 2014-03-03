# PigPen Tutorial

Getting started with Clojure and PigPen is really easy. Just follow the steps below to get up and running.

  1. Install [Leiningen](https://github.com/technomancy/leiningen#leiningen)
  2. Create a new leiningen project with `lein new pigpen-demo`. This will create a pigpen-demo folder for your project.
  3. Add PigPen as a dependency by changing the dependencies in your project's `project.clj` file to look like this:

    ``` clj
      :dependencies [[org.clojure/clojure "1.5.1"]
                     [com.netflix.pigpen/pigpen "0.2.0"]]
      :profiles {:dev {:dependencies [[org.apache.pig/pig "0.11.1"]
                                      [org.apache.hadoop/hadoop-core "1.1.2"]]}}
    ```

  4. Run `lein repl` to start a REPL for your new project.
  5. Try some samples below...

If you have any questions, or if something doesn't look quite right, contact us here: pigpen-support@googlegroups.com

_Note: If you are not familiar at all with [Clojure](http://clojure.org/), I strongly recommend that you try a tutorial [here](http://tryclj.com/), [here](http://java.ociweb.com/mark/clojure/article.html), or [here](http://learn-clojure.com/) to understand some of the [basics](http://clojure.org/cheatsheet)._

_Note: PigPen requires Clojure 1.5.1 or greater. The Leiningen example uses Leiningen 2.0 or greater._

To get started, we import the pigpen.core namespace:

``` clj
(require '[pigpen.core :as pig])
```

First, lets load some data. Text files (tsv, csv) can be read using [`pig/load-tsv`](http://netflix.github.io/PigPen/pigpen.core.html#var-load-tsv). If you have Clojure data, take a look at [`pig/load-clj`](http://netflix.github.io/PigPen/pigpen.core.html#var-load-clj).

The following code defines a function that returns a query. This query loads data from the file input.tsv.

``` clj
(defn my-data []
  (pig/load-tsv "input.tsv"))
```

_Note: If you call this function, it will just return the PigPen representation of a query. To really use it, you'll need to execute it locally or convert it to a script (more on that later)._

We can test our query in a REPL like so... First, create some test data:

``` clj
=> (spit "input.tsv" "1\t2\tfoo\n4\t5\tbar")
```

And then run the script to return our data:

``` clj
=> (pig/dump (my-data))
[["1" "2" "foo"] ["4" "5" "bar"]]
```

Now let's transform our data:

``` clj
(defn my-data []
  (->>
    (pig/load-tsv "input.tsv")
    (pig/map (fn [[a b c]]
               {:sum (+ (Integer/valueOf a) (Integer/valueOf b))
                :name c}))))
```

If we run the script now, our output data reflects the transformation:

``` clj
=> (pig/dump (my-data))
[{:sum 3, :name "foo"} {:sum 9, :name "bar"}]
```

And we can filter the data too:

``` clj
(defn my-data []
  (->>
    (pig/load-tsv "input.tsv")
    (pig/map (fn [[a b c]]
               {:sum (+ (Integer/valueOf a) (Integer/valueOf b))
                :name c}))
    (pig/filter (fn [{:keys [sum]}]
                  (< sum 5)))))

=> (pig/dump (my-data))
[{:sum 3, :name "foo"}]
```

It's generally a good practice to separate the loading of the data from our business logic. Let's separate our script into multiple functions and add a store operator:

``` clj
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
```

Now we can define a unit test for our query:

``` clj
(use 'clojure.test)

(deftest test-my-func
  (let [data (pig/return [["1" "2" "foo"] ["4" "5" "bar"]])]
    (is (= (pig/dump (my-func data))
           [{:sum 3, :name "foo"}]))))
```

The function [`pig/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump) takes any PigPen query, executes it locally, and returns the data. Note that [`pig/store`](http://netflix.github.io/PigPen/pigpen.core.html#var-store-tsv) commands return `[]`.

If we want to generate a script, that's easy too:

``` clj
(pig/write-script "my-script.pig" (my-query "input.tsv" "output.clj"))
```

We can optionally run our script locally in Pig (if you have it installed, which is a not a requirement of PigPen). The easiest way to build the pigpen jar is to build an uberjar for our project. From the command line:

```
$ lein uberjar
$ cp target/pigpen-demo-0.1.0-SNAPSHOT-standalone.jar pigpen.jar
$ pig -x local -f my-script.pig
$ cat output.clj/part-m-00000
{:sum 3, :name "foo"}
```

_Note: Pig can't overwrite files, so you'll need to delete this folder to run again. Another recommended option is to put a timestamp in the path._

We'll use unit tests from here on to demonstrate a few more commands. 

Take a look at `command`:

``` clj
(deftest test-join
  (let [left  (pig/return [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
        right (pig/return [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])

        command (pig/join [(left :on :a)
                           (right :on :c)]
                          (fn [l r] [(:b l) (:d r)]))]

    (is (= (pig/dump command)
           [[2 "foo"]
            [3 "foo"]
            [4 "bar"]
            [4 "baz"]]))))
```

This is how to join data in PigPen. You can specify 2 or more relations to join. See the docs for options regarding outer joins and nil handling. The last argument is the consolidation function - for each record from `left` & `right` that are joined, this function is called to merge the results.

Note that the return type of this function isn't a map. You can return anything from a PigPen function - vectors, strings, anything. Maps are generally the easiest to destructure in the next operator, but it's entirely up to you.

Next is a common map-reduce pattern - co-group:

``` clj
(deftest test-cogroup
  (let [left  (pig/return [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
        right (pig/return [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])

        command (pig/cogroup [(left :on :a)
                              (right :on :c)]
                             (fn [k l r] [k (map :b l) (map :d r)]))]

    (is (= (pig/dump command)
           [[1 [2 3] ["foo"]]
            [2 [4]   ["bar" "baz"]]]))))
```

A cogroup is similar to a join, but instead of flattening the matching rows, all of the values are passed to the consolidation function. Note that our function takes 3 arguments - the first one is the key was joined, the rest are collections of the values that match the key for each of the relations.

It is quite common in map-reduce for the individual groups in a group-by or cogroup to be very large. In these cases, we may need to incrementally aggregate the data in parallel. To accomplish this, we use the functions in pigpen.fold. Check out the pigpen.fold namespace for more info and usage.
