![](logo.png)

PigPen is a map-reduce abstraction for Clojure. It is written on top of [Apache Pig](http://pig.apache.org/), but you don't need to know much about Pig to use it.

##Goals of PigPen:

  * A map-reduce query abstraction that's as similar as possible to clojure.core. The idea is to feel like you're working with local Clojure data when writing a map-reduce job.
  * Allow for iterative development of scripts using standard unit testing
  * Map-reduce using a dynamic language (no types, but it just works)

## Motivations for creating PigPen:

  * __Organize all of our related code in one place.__ We don't like switching between a script and a UDF written in different languages. We don't want to think about mapping between differing data types in different languages.
  * __Code re-usability.__ We want to be able to define a piece of logic once, parametrize it, and reuse it for many different jobs. We want our code in multiple files, organized in a coherent manner - not dictated by the job it belongs to.
  * __Unit testing.__ We want our sample data inline with our unit tests. We want our unit tests to test the business logic of our jobs without complications of loading or storing data. We want to be able to trivially inject mock data at any point in our jobs. We want to be able to quickly test a query without waiting for a JVM to start.
  * __Name what you want to.__ Most map-reduce languages force too many names and schemas on intermediate products. This can make it really difficult to mock data and test isolated portions of jobs. We want to organize and name our business logic as we see fit - not as dictated by the language.
  * __We're done writing scripts; we're ready to start programming.__

_Note: PigPen is __not__ a Clojure wrapper for writing Pig scripts you can hand edit. While it's entirely possible, the resulting scripts are not intended for human consumption._

_Note: If you are not familiar at all with [Clojure](http://clojure.org/), I strongly recommend that you try a tutorial [here](http://tryclj.com/), [here](http://java.ociweb.com/mark/clojure/article.html), or [here](http://learn-clojure.com/) to understand some of the [basics](http://clojure.org/cheatsheet)._

# Really, yet another map-reduce language?

### If you know Clojure, you already know PigPen

The primary goal of PigPen is to take language out of the equation. PigPen operators are designed to be as close as possible to the Clojure equivalents. And there are no sidecar files for UDFs - they're right inline where they should be.

Here's the proverbial word count:

``` clj

    (require '[pigpen.core :as pig])

    (defn word-count [lines]
      (->> lines
        (pig/mapcat #(-> % first
                       (clojure.string/lower-case)
                       (clojure.string/replace #"[^\w\s]" "")
                       (clojure.string/split #"\s+")))
        (pig/group-by identity)
        (pig/map (fn [[word occurrences]] [word (count occurrences)]))))
```

As you can see, this is just the word count logic. We don't have to conflate external concerns, like where our data is coming from or going.


# Will it compose?

Yep - PigPen queries are written as function compositions - data in, data out. Write it once and avoid the copy & paste routine.

Here we use word-count to make a PigPen query:

``` clj

    (defn word-count-query [input output]
      (->>
        (pig/load-tsv input)
        (word-count)
        (pig/store-tsv output)))
```

This function returns the PigPen representation of the query. By itself, it won't do anything - we have to execute it locally or generate a script (more on that later).


# You like unit tests? Yeah, we do that

With PigPen, you can mock out data and write a unit test for your query. No more crossing your fingers & wondering what will happen when you submit to the cluster. No more separate files for test input & output.

Mocking is super easy - run the first part of the query to produce some sample rows. Then copy & paste the sample data into your unit test.

``` clj

    (deftest test-word-count
      (let [data (pig/return [["The fox jumped over the dog."]
                              ["The cow jumped over the moon."]])]
        (is (= (pig/dump (word-count data))
               [["moon" 1] ["jumped" 2] ["dog" 1] ["over" 2] ["cow" 1] ["fox" 1] ["the" 4]]))))
```

The [`pig/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump) operator is what runs the query locally.

# Closures (yes, the kind with an S)

Parameterizing your query is trivial. Any in-scope function parameters or let bindings are available to use in functions.

``` clj

    (defn reusable-fn [lower-bound data]
      (let [upper-bound (+ lower-bound 10)]
        (pig/filter (fn [x] (< lower-bound x upper-bound)) data)))
```

_Note: To exclude a local variable, add the metadata ^:local to the declaration._

# So how do I use it?

Just tell PigPen where to write the query as a Pig script:

``` clj

    (pig/write-script "word-count.pig" (word-count-query "input.tsv" "output.tsv"))
```

And then you have a Pig script which you can submit to your cluster.

As you saw before, we can also use [`pig/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump) to run the query locally and return Clojure data:

``` clj

    (pig/dump (word-count data))
```

Tutorials & Documentation

There are three distinct audiences for PigPen, so we wrote three different tutorials:

  * Those coming from the Clojure community who want to do map-reduce: [PigPen for Clojure users](README-for-Clojure-Users.md)
  * Those coming from the Pig community who want to use Clojure: [PigPen for Pig users](README-for-Pig-Users.md)
  * And a general tutorial for anybody wanting to learn it all (below)

If you know both Clojure and Pig, you'll probably find all of tutorials interesting.

The full API documentation is located [here](http://netflix.github.io/PigPen/pigpen.core.html)

# PigPen Tutorial

## Getting started

Getting started with Clojure and PigPen is really easy. Just follow the steps below to get up and running.

  1. Install [Leiningen](https://github.com/technomancy/leiningen#leiningen)
  2. Create a new leiningen project with `lein new pigpen-demo`
  3. Add PigPen as a dependency by adding `[com.netflix.pigpen/pigpen "0.1.0"]` into your project's `project.clj` file.
  4. Run `lein repl` to start a REPL for your new project.
  5. Try some samples below...

_Note: If you are not familiar at all with [Clojure](http://clojure.org/), I strongly recommend that you try a tutorial [here](http://tryclj.com/), [here](http://java.ociweb.com/mark/clojure/article.html), or [here](http://learn-clojure.com/) to understand some of the [basics](http://clojure.org/cheatsheet)._

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

    (deftest test-my-func
      (let [data (pig/return [[1 2 "foo"] [4 5 "bar"]])]
        (is (= (pig/dump (my-func data))
               [{:sum 3, :name "foo"}]))))
```

The function [`pig/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump) takes any PigPen query, executes it locally, and returns the data. Note that [`pig/store`](http://netflix.github.io/PigPen/pigpen.core.html#var-store-tsv) commands return `[]`.

If we want to generate a script, that's easy too:

``` clj

    (pig/write-script "my-script.pig" (my-query "input.tsv" "output.clj"))
```

We'll use unit tests from here on to demonstrate a few more commands. 

Take a look at `command`:

``` clj

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
```

This is how to join data in PigPen. You can specify 2 or more relations to join. See the docs for options regarding outer joins and nil handling. The last argument is the consolidation function - for each record from `left` & `right` that are joined, this function is called to merge the results.

Note that the return type of this function isn't a map. You can return anything from a PigPen function - vectors, strings, anything. Maps are generally the easiest to destructure in the next operator, but it's entirely up to you.

Next is a common map-reduce pattern - co-group:

``` clj

    (deftest test-cogroup
      (let [left  (pig/return [{:a 1 :b 2} {:a 1 :b 3} {:a 2 :b 4}])
            right (pig/return [{:c 1 :d "foo"} {:c 2 :d "bar"} {:c 2 :d "baz"}])

            command (pig/cogroup (left on :a)
                                 (right on :c)
                                 (fn [k l r] [k (map :b l) (map :d r)]))]

        (is (= (pig/dump command)
               [[1 [2 3] ["foo"]]
                [2 [4]   ["bar" "baz"]]]))))
```

A cogroup is similar to a join, but instead of flattening the matching rows, all of the values are passed to the consolidation function. Note that our function takes 3 arguments - the first one is the key was joined, the rest are collections of the values that match the key for each of the relations.


# Why Pig?

Pig was chosen because we didn't want to have to redo all of the optimization work that Pig has done already. If you strip away the language, Pig does an excellent job of moving big data around.

Long term, if performance issues crop up, it will be relatively easy to migrate to running PigPen directly on Hadoop without changing the abstraction. So far, performance doesn't seem to be an issue.

It would be possible to do a similar thing on top of Hive, but schematizing data doesn't fit well with the Clojure ideology. Also, Hive is similar to SQL, making translation from a functional language more difficult. Pig is by nature more functional and supports a lot of concepts like map, filter, and reduce right out of the box.

Scalding looks nice, but our team is a Clojure shop. It could be said that this is the Clojure equivalent of Scalding.

Cascalog is usually the go-to language for map-reduce in Clojure, but from past experiences datalog has proven less than useful for everyday tasks. There's a lot of new syntax and concepts to learn, adjusting names to do an implicit join is not always ideal (or practical), misplaced ordering of operations can often cause big performance problems, datalog will flatten data structures (which can be wasteful), and composition can be a mind bender.
