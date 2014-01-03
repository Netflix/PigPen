# PigPen for Clojure Users

## Artifacts

`pigpen` is available from Maven:

With Leiningen:

``` clj
[com.netflix.pigpen/pigpen "0.1.1"]
```

With Maven:

``` xml
<dependency>
  <groupId>com.netflix.pigpen</groupId>
  <artifactId>pigpen</artifactId>
  <version>0.1.1</version>
</dependency>
```

_Note: Make sure you're using Clojure 1.5.1 or greater_

## Operators

Most of the usual operators you use on seqs ([`map`](http://netflix.github.io/PigPen/pigpen.core.html#var-map), [`mapcat`](http://netflix.github.io/PigPen/pigpen.core.html#var-mapcat), [`filter`](http://netflix.github.io/PigPen/pigpen.core.html#var-filter), [`reduce`](http://netflix.github.io/PigPen/pigpen.core.html#var-reduce), [`group-by`](http://netflix.github.io/PigPen/pigpen.core.html#var-group-by), [`into`](http://netflix.github.io/PigPen/pigpen.core.html#var-into), [`take`](http://netflix.github.io/PigPen/pigpen.core.html#var-take)) have PigPen equivalents. Check out the [full docs](http://netflix.github.io/PigPen/pigpen.core.html) for the whole list of what is supported.

_Note: The operators don't return the actual data. They return an expression tree that can be later translated into either a script or run locally._

Here's what's different and/or new in PigPen:

### Load / Store

The load operators in PigPen are kind of like a reader plus line-seq that returns a lazy sequence that's disposed of when you finish reading. Conceptually, you can just think of it as something that returns a lazy seq you don't have to worry about closing.

``` clj
(pig/load-tsv "input.tsv")
```

In this example, we're reading Pig data structures from the file input.tsv. If our input looked like this:

```
1   2   3
4   5   6
```

Our output would be this:

``` clj
([1 2 3]
 [4 5 6])
```

There are also loaders that will read Clojure and other formats. Check out the docs for more examples.

Storing data is like a line-by-line writer that's similar to clojure.core/spit:

There are a few storage options:

  * [`(pig/store-clj "output.clj" my-relation)`](http://netflix.github.io/PigPen/pigpen.core.html#var-store-clj) - Stores the relation as EDN
  * [`(pig/store-tsv "output.tsv" my-relation)`](http://netflix.github.io/PigPen/pigpen.core.html#var-store-tsv) - Stores the relation as a tsv file. Each element must be a seq and each value is a column.
  * [`(pig/store-pig "output.pig" my-relation)`](http://netflix.github.io/PigPen/pigpen.core.html#var-store-pig) - Stores the relation in Pig format (not recommended - not idempotent)

Check out the docs for more examples.

### Joins

List comprehensions (`clojure.core/for`) aren't supported because they don't translate very well into map-reduce. Instead, we opted to go with [`pig/join`](http://netflix.github.io/PigPen/pigpen.core.html#var-join):

``` clj
(pig/join (foo on :a)
          (bar on (fn [bar] (-> bar second str)))
          (fn [foo bar] (vector foo bar)))
```

  * `foo` and `bar` are the relations we're joining
  * `:a` is the key selector for `foo`
  * `bar` uses a slightly more complex key selector - any function is allowed here
  * The last argument is a function of two arguments that is used to combine each joined `foo` with each joined `bar`

There are many options and variants for joins. Look at [`pig/cogroup`](http://netflix.github.io/PigPen/pigpen.core.html#var-cogroup) also - it's like a join without the flatten. Check out the [full docs](http://netflix.github.io/PigPen/pigpen.core.html) for more examples.

### Set operations

Clojure has clojure.set, but it doesn't handle multi-set operations. PigPen has both:

  * [`pig/union`](http://netflix.github.io/PigPen/pigpen.core.html#var-union) - The distinct set of all elements in all relations. Similar to `clojure.set/union`
  * [`pig/union-multiset`](http://netflix.github.io/PigPen/pigpen.core.html#var-union-multiset) - Similar to `clojure.core/concat`, but there's no sense of order in most of map-reduce
  * [`pig/intersection`](http://netflix.github.io/PigPen/pigpen.core.html#var-intersection) - Similar to `clojure.set/intersection`
  * [`pig/intersection-multiset`](http://netflix.github.io/PigPen/pigpen.core.html#var-intersection-multiset) - Computes the multiset intersection
  * [`pig/difference`](http://netflix.github.io/PigPen/pigpen.core.html#var-difference) - Similar to `clojure.set/difference`
  * [`pig/difference-multiset`](http://netflix.github.io/PigPen/pigpen.core.html#var-multiset) - Computes the multiset difference

 Check out the [docs](http://netflix.github.io/PigPen/pigpen.core.html) for examples of how multiset operations are different.

### Sample

When you want to restrict the amount of data used in a map-reduce job, it's often favorable to sample the data instead of limiting it. This is due to the distributed nature of the execution. If you ask for 1000 rows and you're running the job on 1000 machines, each of those machines must output 1000 rows and send it to a single node that actually limits the output to 1000 rows. This is because any of those 1000 nodes may return 0 rows - none of them knows what the other is doing.

A better approach is to [sample](http://netflix.github.io/PigPen/pigpen.core.html#var-sample) the data by taking a random percentage of it.

``` clj
(pig/sample 0.01 my-relation)
```

This command will take 1% of the data at random.

Note: PigPen also supports [`pig/take`](http://netflix.github.io/PigPen/pigpen.core.html#var-take), which is much more useful when running locally to debug a script.

## Local Mode

When run locally, pigpen is translated into [rx](https://github.com/Netflix/RxJava), though you won't see any Observables unless you want to.

To run locally, use [`pig/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump). This will translate the PigPen into rx and execute it.

  * If the last statement is a relation, data is returned
  * If the last statement is a store command, the data is written to a file and `[]` is returned
  * If the last statement is a script command, the output is the result of merging the relations together (usually `[]` from multiple store commands)
