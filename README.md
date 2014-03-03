![](logo.png)

PigPen is map-reduce for Clojure. It compiles to [Apache Pig](http://pig.apache.org/), but you don't need to know much about Pig to use it.

##What is PigPen?

  * A map-reduce language that looks and behaves like clojure.core
  * The ability to write map-reduce queries as programs, not scripts
  * Strong support for unit tests and iterative development

## Really, yet another map-reduce language?

### If you know Clojure, you already know PigPen

The primary goal of PigPen is to take language out of the equation. PigPen operators are designed to be as close as possible to the Clojure equivalents. There are no special user defined functions (UDFs). Define Clojure functions, anonymously or named, and use them like you would in any Clojure program.

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

This defines a function that returns a PigPen query expression. The query takes a sequence of lines and returns the frequency that each word appears. As you can see, this is just the word count logic. We don't have to conflate external concerns, like where our data is coming from or going to.


## Will it compose?

Yep - PigPen queries are written as function compositions - data in, data out. Write it once and avoid the copy & paste routine.

Here we use our word-count function (defined above), along with a load and store command, to make a PigPen query:

``` clj
(defn word-count-query [input output]
  (->>
    (pig/load-tsv input)
    (word-count)
    (pig/store-tsv output)))
```

This function returns the PigPen representation of the query. By itself, it won't do anything - we have to execute it locally or generate a script (more on that later).

## You like unit tests? Yeah, we do that

With PigPen, you can mock input data and write a unit test for your query. No more crossing your fingers & wondering what will happen when you submit to the cluster. No more separate files for test input & output.

Mocking data is really easy. With [`pig/return`](http://netflix.github.io/PigPen/pigpen.core.html#var-return) and [`pig/constantly`](http://netflix.github.io/PigPen/pigpen.core.html#var-constantly), you can inject arbitrary data as a starting point for your script.

A common pattern is to use [`pig/take`](http://netflix.github.io/PigPen/pigpen.core.html#var-take) to sample a few rows of the actual source data. Wrap the result with [`pig/return`](http://netflix.github.io/PigPen/pigpen.core.html#var-return) and you've got mock data.

``` clj
(use 'clojure.test)

(deftest test-word-count
  (let [data (pig/return [["The fox jumped over the dog."]
                          ["The cow jumped over the moon."]])]
    (is (= (pig/dump (word-count data))
           [["moon" 1] ["jumped" 2] ["dog" 1] ["over" 2] ["cow" 1] ["fox" 1] ["the" 4]]))))
```

The [`pig/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump) operator runs the query locally.

## Closures (yes, the kind with an S)

Parameterizing your query is trivial. Any available functions, in-scope function parameters, or let bindings are available to use in functions.

``` clj
(defn inc-two [x]
  (+ x 2))

(defn reusable-fn [lower-bound data]
  (let [upper-bound (+ lower-bound 10)]
    (->> data
      (pig/filter (fn [x] (< lower-bound x upper-bound)))
      (pig/map inc-two))))
```

Note that `inc-two`, `lower-bound`, and `upper-bound` are present when we generate the script, and are made available when the function is executed within the cluster.

_Note: To exclude a local variable, add the metadata ^:local to the declaration._

## So how do I use it?

Just tell PigPen where to write the query as a Pig script:

``` clj
(pig/write-script "word-count.pig" (word-count-query "input.tsv" "output.tsv"))
```

And now you have a Pig script which you can submit to your cluster. The script uses `pigpen.jar`, an uberjar with all of the required dependencies along with your code. The easiest way to create this jar is to build an uberjar for your project. Check out the [tutorial](Tutorial.md) for how to build an uberjar and run the script in Pig.

As you saw before, we can also use [`pig/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump) to run the query locally and return Clojure data:

``` clj
=> (def data (pig/return [["The fox jumped over the dog."]
                          ["The cow jumped over the moon."]]))
#'pigpen-demo/data

=> (pig/dump (word-count data))
[["moon" 1] ["jumped" 2] ["dog" 1] ["over" 2] ["cow" 1] ["fox" 1] ["the" 4]]
```

# Getting Started, Tutorials & Documentation

Getting started with Clojure and PigPen is really easy. The [tutorial](Tutorial.md) is the best way to get Clojure and PigPen installed and start writing queries.

_Note: If you are not familiar at all with [Clojure](http://clojure.org/), I strongly recommend that you try a tutorial [here](http://tryclj.com/), [here](http://java.ociweb.com/mark/clojure/article.html), or [here](http://learn-clojure.com/) to understand some of the [basics](http://clojure.org/cheatsheet)._

There are three distinct audiences for PigPen, so we wrote three different tutorials:

  * Those coming from the Clojure community who want to do map-reduce: [PigPen for Clojure users](README-for-Clojure-Users.md)
  * Those coming from the Pig community who want to use Clojure: [PigPen for Pig users](README-for-Pig-Users.md)
  * And a general tutorial for anybody wanting to learn it all: [PigPen Tutorial](Tutorial.md)

If you know both Clojure and Pig, you'll probably find all of the tutorials interesting.

The full API documentation is located [here](http://netflix.github.io/PigPen/pigpen.core.html)

Questions & Complaints: pigpen-support@googlegroups.com

_Note: PigPen is **not** a Clojure wrapper for writing Pig scripts you can hand edit. While it's entirely possible, the resulting scripts are not intended for human consumption._

## Artifacts

`pigpen` is available from Maven:

With Leiningen:

``` clj
[com.netflix.pigpen/pigpen "0.2.0"]
```

With Gradle:

``` groovy
compile "com.netflix.pigpen:pigpen:0.2.0"
```

With Maven:

``` xml
<dependency>
  <groupId>com.netflix.pigpen</groupId>
  <artifactId>pigpen</artifactId>
  <version>0.2.0</version>
</dependency>
```

_Note: PigPen requires Clojure 1.5.1 or greater_

### Release Notes

  * 0.2.0 - Added pigpen.fold - Note: this includes a breaking change in the join and cogroup syntax as follows:
    
    ``` clj
    ; before
    (pig/join (foo on :f)
              (bar on :b optional)
              (fn [f b] ...))
    
    ; after
    (pig/join [(foo :on :f)
               (bar :on :b :type :optional)]
              (fn [f b] ...))
    ```
    
    Each of the select clauses must now be wrapped in a vector - there is no longer a varargs overload to either of these forms. Within each of the select clauses, :on is now a keyword instead of a symbol, but a symbol will still work if used. If `optional` or `required` were used, they must be updated to `:type :optional` and `:type :required`, respectively.
    
  * 0.1.5 - Performance improvements: implemented Pig's Accumulator interface; tuned nippy; reduced number of times data is serialized.
  * 0.1.4 - Fix sort bug in local mode
  * 0.1.3 - Change Pig & Hadoop to be transitive dependencies. Add support for consuming user code via closure.
  * 0.1.2 - Upgrade instaparse to 1.2.14
  * 0.1.1 - Initial Release
