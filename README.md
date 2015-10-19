![](logo.png)

PigPen is map-reduce for Clojure, or distributed Clojure. It compiles to [Apache Pig](http://pig.apache.org/) or [Cascading](http://www.cascading.org/) but you don't need to know much about either of them to use it.

# Getting Started, Tutorials & Documentation

Getting started with Clojure and PigPen is really easy.

  * The [wiki](https://github.com/Netflix/PigPen/wiki) explains what PigPen does and why we made it
  * The [tutorial](https://github.com/Netflix/PigPen/wiki/Tutorial) is the best way to get Clojure and PigPen installed and start writing queries
  * The [full API](http://netflix.github.io/PigPen/pigpen.core.html) lists all of the operators with example usage
  * [PigPen for Clojure users](https://github.com/Netflix/PigPen/wiki/Getting_Started_for_Clojure_Users) is great for Clojure users new to map-reduce
  * [PigPen for Pig users](https://github.com/Netflix/PigPen/wiki/Getting_Started_for_Pig_Users) is great for Pig users new to Clojure
  * [PigPen for Cascading users](https://github.com/Netflix/PigPen/wiki/Getting_Started_for_Cascading_Users) is great for Cascading users new to Clojure

_Note: It is strongly recommended to familiarize yourself with Clojure before using PigPen._

_Note: PigPen is **not** a Clojure wrapper for writing Pig scripts you can hand edit. While entirely possible, the resulting scripts are not intended for human consumption._

# Questions & Complaints

  * pigpen-support@googlegroups.com

# Artifacts

`pigpen` is available from Maven:

With Leiningen:

``` clj
;; core library
[com.netflix.pigpen/pigpen "0.3.1"]

;; pig support
[com.netflix.pigpen/pigpen-pig "0.3.1"]

;; cascading support
[com.netflix.pigpen/pigpen-cascading "0.3.1"]

;; rx support
[com.netflix.pigpen/pigpen-rx "0.3.1"]
```

The platform libraries all reference the core library, so you only need to reference the platform specific one that you require and the core library should be included transitively.

_Note: PigPen requires Clojure 1.5.1 or greater_

## Parquet

To use the parquet loader, add this to your dependencies:

``` clj
[com.netflix.pigpen/pigpen-parquet-pig "0.3.1"]
```

And check out the [`pigpen.parquet`](http://netflix.github.io/PigPen/pigpen.parquet.html) namespace for usage.

_Note: Avro is currently only supported by Pig_

## Avro

To use the avro loader (alpha), add this to your dependencies:

``` clj
[com.netflix.pigpen/pigpen-avro-pig "0.3.1"]
```

And check out the [`pigpen.avro`](http://netflix.github.io/PigPen/pigpen.avro.html) namespace for usage.

_Note: Avro is currently only supported by Pig_

# Release Notes

  * 0.3.1 - 10/19/15
    * Update cascading version to 2.7.0
    * Report correct pigpen version to concurrent
    * Update nippy to 2.10.0 & tune performance
  * 0.3.0 - 5/18/15
    * No changes
  * 0.3.0-rc-7 - 4/29/15
    * Fixed bug in local mode where nils weren't handled consistently
  * 0.3.0-rc.6 - 4/14/15
    * Add local mode code eval memoization to avoid thrashing permgen
    * Added [`pigpen.pig/set-options`](http://netflix.github.io/PigPen/pigpen.pig.html#var-set-options) command to explicitly set pig options in a script. This was previously available (though undocumented) by setting `{:pig-options {...}}` in any options block, but is now official.
  * 0.3.0-rc.5 - 4/9/15
    * Update core.async version
  * 0.3.0-rc.4 - 4/8/15
    * Memoize code evaluation when run in the cluster
  * 0.3.0-rc.3 - 4/2/15
    * Bugfixes
  * 0.3.0-rc.2 - 3/30/15
    * Parquet refactor. Local parquet loading no longer depends on Pig. Parquet schemas are now defined using Parquet classes.
  * 0.3.0-rc.1 - 3/23/15
    * Added Cascading support
      * [`pigpen.cascading/generate-flow`](http://netflix.github.io/PigPen/pigpen.cascading.html#var-generate-flow) - Generate a cascading flow from a pigpen query
      * [`pigpen.cascading/load-tap`](http://netflix.github.io/PigPen/pigpen.cascading.html#var-load-tap) - Load data from an existing cascading tap
      * [`pigpen.cascading/store-tap`](http://netflix.github.io/PigPen/pigpen.cascading.html#var-store-tap) - Store data using an existing cascading tap
    * Added [`pigpen.core/keys-fn`](http://netflix.github.io/PigPen/pigpen.core.html#var-keys-fn), a new convenience macro to support named anonymous functions. Like keys destructuring, but less verbose.
    * New function based operators to build more dynamic scripts. These are function versions of all the core pigpen macros, but you have to handle quoting user code manually. These were previously available, but not officially supported. Now they're alpha, but supported and documented. See [`pigpen.core.fn`](http://netflix.github.io/PigPen/pigpen.core.fn.html)
    * New lower-level operators to build custom storage and commands. These were previously available, but not officially supported. Now they're alpha, but supported and documented. See [`pigpen.core.op`](http://netflix.github.io/PigPen/pigpen.core.op.html)
    * __*** Breaking Changes ***__
      * `pigpen.core/script` is now [`pigpen.core/store-many`](http://netflix.github.io/PigPen/pigpen.core.html#var-store-many)
      * `pigpen.core/generate-script` is now [`pigpen.pig/generate-script`](http://netflix.github.io/PigPen/pigpen.pig.html#var-generate-script)
      * `pigpen.core/write-script` is now [`pigpen.pig/write-script`](http://netflix.github.io/PigPen/pigpen.pig.html#var-write-script)
      * `pigpen.core/show` is now [`pigpen.viz/show`](http://netflix.github.io/PigPen/pigpen.viz.html#var-show) (requires dependency `[com.netflix.pigpen/pigpen-viz "..."]`)
      * `pig/dump` has changed. The old version was based on rx-java, and still exists as [`pigpen.rx/dump`](http://netflix.github.io/PigPen/pigpen.rx.html#var-dump). The replacement for [`pigpen.core/dump`](http://netflix.github.io/PigPen/pigpen.core.html#var-dump) is now entirely Clojure based. The Clojure version is better for unit tests and small data. All stages are evaluated eagerly, so the stack traces are simpler to read. The rx version is lazy, including the load-* commands. This means that you can load a large file, take a few rows, and process them without loading the entire file into memory. The downside is confusing stack traces and extra dependencies. See [here](https://github.com/Netflix/PigPen/wiki/Local_Evaluation) for more details.
      * The interface for building custom loaders and storage has changed. See [here](https://github.com/Netflix/PigPen/wiki/Custom_Loaders) for more details. Please email pigpen-support@googlegroups.com with any questions.
  * 0.2.15 - 2/20/15
    * Include sources in jars
  * 0.2.14 - 2/18/15
    * Avro updates
  * 0.2.13 - 1/19/15
    * Added `load-avro` in the pigpen-avro project: http://avro.apache.org/
    * Fixed the nRepl configuration; use `gradlew nRepl` to start an nRepl
    * Exclude nested relations from closures
  * 0.2.12 - 12/16/14
    * Added `load-csv`, which allows for quoting per RFC 4180
  * 0.2.11 - 10/24/14
    * Fixed a bug (feature?) introduced by new rx version. Also upgraded to rc7. This would have only affected local mode where the data being read was faster than the code consuming it.
  * 0.2.10 - 9/21/14
    * Removed load-pig and store-pig. The pig data format is very bad and should not be used. If you used these and want them back, email pigpen-support@googlegroups.com and we'll put it into a separate jar. The jars required for this feature were causing conflicts elsewhere.
    * Upgraded the following dependencies:
      * org.clojure/clojure 1.5.1 -> 1.6.0 - this was also changed to a provided dependency, so you should be able to use any version greater than 1.5.1
      * org.clojure/data.json 0.2.2 -> 0.2.5
      * com.taoensso/nippy 2.6.0-RC1 -> 2.6.3
      * clj-time 0.5.0 - no longer needed
      * joda-time 2.2 -> 2.4 - pig needs this to run locally
      * instaparse 1.2.14 - no longer needed
      * io.reactivex/rxjava 0.9.2 -> 1.0.0-rc.1
    * Fixed the rx limit bug. `pigpen.local/*max-load-records*` is no longer required.
  * 0.2.9 - 9/16/14
    * Fix a local-mode bug in `pigpen.fold/avg` where some collections would produce a NPE.
    * Change fake pig delimiter to \n instead of \0. Allows for \0 to exist in input data.
    * Remove 1000 record limit for local-mode. This was originally introduced to mitigate an rx bug. Until #61 is fixed, bind `pigpen.local/*max-load-records*` to the maximum number of records you want to read locally when reading large files. This now defaults to `nil` (no limit).
    * Fix a local dispatch bug that would prevent loading folders locally
  * 0.2.8 - 7/31/14
    * Fix a bug in `load-tsv` and `load-lazy`
  * 0.2.7 - 7/31/14 *** Don't use ***
    * Fix `load-lazy` and speed up both `load-tsv` and `load-lazy`
    * Convert to multi-project build
    * Added pigpen-parquet with initial support for loading the Parquet format: https://github.com/apache/incubator-parquet-mr
  * 0.2.6 - 6/17/14
    * Minor optimization for local mode. The creation of a UDF was occurring for every value processed, causing it to run out of perm-gen space when processing large collections locally.
    * Fix `(pig/return [])`
    * Fix `(pig/dump (pig/reduce + (pig/return [])))`
    * Fix `Long`s in scripts that are larger than an Integer
    * Memoize local UDF instances per use of `pig/dump`
    * The jar location in the generated script is now configurable. Use the `:pigpen-jar-location` option with `pig/generate-script` or `pig/write-script`.
  * 0.2.5 - 4/9/14
    * Remove `dump&show` and `dump&show+` in favor of `pigpen.oven/bake`. Call `bake` once and pass to as many outputs as you want. This is a breaking change, but I didn't increment the version because `dump&show` was just a tool to be used in the REPL. No scripts should break because of this change.
    * Remove `dymp-async`. It appeared to be broken and was a bad idea from the start.
    * Fix self-joins. This was a rare issue as a self join (with the same key) just duplicates data in a very expensive way.
    * Clean up functional tests
    * Fix `pigpen.oven/clean`. When it was pruning the graph, it was also removing REGISTER commands.
  * 0.2.4 - 4/2/14
    * Fix arity checking bug (affected varargs fns)
    * Fix cases where an Algebraic fold function was falling back to the Accumulator interface, which was not supported. This affected using `cogroup` with `fold` over multiple relations.
    * Fix debug mode (broken in 0.1.5)
    * Change UDF initialization to not rely on memoization (caused stale data in REPL)
    * Enable AOT. Improves cluster perf
    * Add `:partition-by` option to `distinct`
  * 0.2.3 - 3/27/14
    * Added `load-json`, `store-json`, `load-string`, `store-string`
    * Added `filter-by`, and `remove-by`
  * 0.2.2 - 3/25/14
    * Fixed bug in `pigpen.fold/vec`. This would also cause `fold/map` and `fold/filter` to not work when run in the cluster.
  * 0.2.1 - 3/24/14
    * Fixed bug when using `for` to generate scripts
    * Fixed local mode bug with `map` followed by `reduce` or `fold`
  * 0.2.0 - 3/3/14
    * Added pigpen.fold - Note: this includes a breaking change in the join and cogroup syntax as follows:

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

  * 0.1.5 - 2/17/14
    * Performance improvements
      * Implemented Pig's Accumulator interface
      * Tuned nippy
      * Reduced number of times data is serialized
  * 0.1.4 - 1/31/14
    * Fix sort bug in local mode
  * 0.1.3 - 1/30/14
    * Change Pig & Hadoop to be transitive dependencies
    * Add support for consuming user code via closure
  * 0.1.2 - 1/3/14
    * Upgrade instaparse to 1.2.14
  * 0.1.1 - 1/3/14
    * Initial Release
