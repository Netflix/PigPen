# PigPen for Pig Users

If you are not familiar at all with [Clojure](http://clojure.org/), I strongly recommend that you find a [tutorial](http://learn-clojure.com/) and understand the [basics](http://clojure.org/cheatsheet). Otherwise a lot of the following will not make any sense.

## Translations

Here are what some common Pig commands look like in PigPen

FOREACH / GENERATE -> [`pig/map`](doc/pigpen.core.html#var-map)

    bar = FOREACH foo GENERATE
        a,
        b + c AS d;

    (pig/map (fn [{:keys [a b c]}]
               {:a a
                :d (+ b c)})
              foo)

FOREACH / GENERATE / FLATTEN -> [`pig/mapcat`](doc/pigpen.core.html#var-mapcat)

    bar = FOREACH foo GENERATE
        FLATTEN(a);

    (pig/mapcat :a foo)

FILTER -> [`pig/filter`](doc/pigpen.core.html#var-filter)

    bar = FILTER foo by a == 1;

    (pig/filter (fn [{:keys [a]}] (= a 1)) foo)

LIMIT -> [`pig/take`](doc/pigpen.core.html#var-take)

    bar = LIMIT foo 1;

    (pig/take 1 foo)

JOIN -> [`pig/join`](doc/pigpen.core.html#var-join)

    baz = JOIN foo BY a, bar BY b;

    (pig/join (foo on :a)
              (bar on :b)
              (fn [f b] ...))

_Note that PigPen's join has an implicit function at the end to combine the rows_

GROUP BY -> [`pig/group-by`](doc/pigpen.core.html#var-group-by)

    bar = GROUP foo BY a;

    (pig/group-by :a foo)

## Pig issues that motivated PigPen

Let's start by looking at word count line by line in Pig

Right off the bat we have code in another file we're referencing. We'll show the contents of that file later...

    REGISTER 'word_count_udf.py' USING jython AS word_count_udf;

Pig says types are optional, but if I used the following, my strings would load as byte arrays, and byte arrays are less than useful representations of strings in python.

    lines = LOAD 'input.tsv';

So we add type info:

    lines = LOAD 'input.tsv' AS (line:chararray);

Now we tokenize:

    tokenized_lines = FOREACH lines GENERATE
        word_count_udf.tokenize(line);

Wait, where's that UDF? Let's jump to python now:

    import re
    import string

    @outputSchema('tokens:{(token:chararray)}')
    def tokenize(line):
        lc = string.lower(line)
        p = re.compile(r'\W+')
        tokens = p.split(lc)
        return filter(lambda t: t != '', tokens)

Probably not the best python - it's not my native tongue. But that's kinda the point.

Notice that our UDF is defining the names & types of the return values. If we don't do that, Pig thinks that our function returns a bytearray. Bytearrays are generally useless if you want to interact with the field.

If Pig thinks that our function returns a bytearray, we then have to cast it back to the actual type in the Pig script. The problem is, we can only cast to simple types, like int or chararray, so for this one we're stuck keeping the type info in the UDF.

We could also move the type info back to the script this way:

    tokenized_lines = FOREACH lines GENERATE
        word_count_udf.tokenize(line) as tokens:{(token:chararray)};

But this is fragile. What this does is tell Pig 'I have exactly this type. If I don't return exactly this type, fail the script.'. This will fail at runtime if it's not exact. Not the end of the world here, but imagine you specify a long here and your UDF returns an int. That will fail at runtime.

So we stick with leaving the type info in another file. Now we need to flatten the token list:

    all_tokens = FOREACH tokenized_lines GENERATE
        FLATTEN(tokens);

Remember our schema back in the UDF? This comes into play here

    tokens:{(token:chararray)}

When we flatten a bag, each element of the bag becomes its own row. But what's the name for that field? It's the one we defined back in the UDF. Had we not defined a name back there, it would still work, but we'd have to refer to the field using positional notation: $0

Another nuance of Pig is that if our UDF returned a tuple, instead of flattening it, it would expand the values into new, nameless fields. It's these kind of weird semantics that can make Pig confusing.

The next step is to group the tokens:

    grouped_tokens = GROUP all_tokens BY token;

Seems simple enough, but the weird thing here is the name of the group key is 'group'. It's a special name that's injected when you group relations.

In the next step, we use this field and count the tokens. Note that we have to use the name of the relation from two steps ago.

    token_counts = FOREACH grouped_tokens GENERATE
        group,
        COUNT(all_tokens.token);

The problem with the naming comes when we want to mock out the data in `grouped_tokens`. There's no way I can find to match the names that are present when we are running the actual script.

And then we store the result:

    STORE token_counts INTO 'output.tsv';

The takeaway from all of this is that names in Pig scripts tend to stretch across the script, making it very difficult to inject mock data without modifying the script. In addition to that, Pig forces you to name every command, which can be very tedious. To contrast, in PigPen you only have to define names where you want them. If you have a bunch of steps to compute data, you can easily thread them together using Clojure threading macros ([->>](http://clojuredocs.org/clojure_core/clojure.core/-%3E%3E)). The names within the data are entirely in your control, making them trivial to mock out without any changes to the script.

### Nil handling

Null handling in Pig is very fragile. A null value in the wrong place that wouldn't affect the output can kill a script that's been running for hours. Clojure has a convention that nils should be handled as soft failures. For example, if you're looking up a key in a map and the map doesn't contain the key, simply return nil instead of throwing an exception.

This plays into how Clojure will count things vs how Pig will. If you have a set of values to count, Pig will fail if one of the values is null. Clojure on the other hand will count it as any other value. If you don't want to count nils, filtering them out is simple as well.

In any place where there was a choice of how to handle nils, we went with the Clojure convention.

### Types in PigPen (or lack thereof)

Types are optional in Pig, but a lot stuff won't work without them. For example, say you load string data without specifying the type and pass it to a UDF. Instead of getting a string, you'll get a list with a tuple that contains each of the bytes of the string.

Pig defines different classes to count objects of different types, so if it happens to lose the type information (via a UDF or FLATTEN command), it can no longer count the items. To make things worse, this is caught at runtime.

Pig has trouble coercing ints to longs. If you give the script a type hint that specifies the value will be a long, but instead you pass it an int, Pig will crash. Clojure doesn't have this issue - transitions between numeric types are handled gracefully.

The Pig parser has trouble with type definitions on multiple lines.

Pig types are confusing. For example, every item in a bag must also be wrapped in a tuple.

None of these are problems for PigPen because it relies on the Clojure type system, where data is just data. A string is always a string, and you can count objects of any type.

### Data format

The Pig data format is not idempotent because it doesn't escape string literals. This means that there's ambiguity when you read the following line. There's no way to tell which comma belongs where, what was a number/boolean, or what was a number/boolean stored as a string.

    [foo,1#123,baz,3#hello, world,bar,2#true,biz,4#true]

Clojure [EDN](https://github.com/edn-format/edn) doesn't have this limitation. You can write it out sans schema and read it back in to get exactly the same data. Unless explicitly specified, PigPen works exclusively with EDN.

Here's the same data in EDN (commas added between elements for clarity):

    {"foo,1" 123, "baz,3" "hello, world", "bar,2" "true", "biz,4" true}

Because of this, when working with Clojure data there is no need to specify a schema for anything.

### Name what you want to

In Pig, every relation must be named:

    bar = FOREACH foo GENERATE ...;

    baz = FILTER bar BY ...;

    baz2 = LIMIT baz 2;

    stuff = FOREACH baz2 GENERATE ...;

Say we now want to take out the LIMIT. Not only do we have to remove that command, we have to keep track of where it was used and update those commands as well.

In PigPen, we can thread these commands together and avoid having to name the intermediate steps. It makes our script much clearer in that 'this is one block of logic'. When programming in any other language, we organize our logic into functions - why should map-reduce be any different?

    (->> foo
      (pig/map ...)
      (pig/filter ...)
      #_(pig/take 2)
      (pig/map ...))

Note also that I can comment out any form (using `#_`) and not have to worry about changing downstream consumers.

Names in Pig stick with relations and fields far after they're relevant. Pig takes the approach of deferring field selection after a join, instead choosing to just qualify the names of the fields. This causes field names to get out of control pretty quickly. Take a look at the following example where we do three joins:

    a = LOAD 'numbers0.tsv' AS (i:int, w:int);
    b = LOAD 'numbers1.tsv' AS (i:int, x:int);
    c = LOAD 'numbers2.tsv' AS (i:int, y:int);
    d = LOAD 'numbers3.tsv' AS (i:int, z:int);

    j0 = JOIN a BY i, b BY i;

    j1 = JOIN j0 BY a::i, c BY i;

    j2 = JOIN j1 BY j0::a::i, d BY i;

The first join, `j0`, is easy - there aren't any conflicting `i`'s in `a` or `b`. When we describe `j0`, we see that we now have two separate fields named `i`:

    j0: {a::i: int,a::w: int,b::i: int,b::x: int}

There's `a::i` and `b::i`, so we need to be aware of that in any subsequent steps, such as `j1`. When we describe `j1`, we see that our problem is getting worse:

    j1: {j0::a::i: int,j0::a::w: int,j0::b::i: int,j0::b::x: int,c::i: int,c::y: int}

And in `j2`:

    j2: {j1::j0::a::i: int,j1::j0::a::w: int,j1::j0::b::i: int,j1::j0::b::x: int,j1::c::i: int,j1::c::y: int,d::i: int,d::z: int}

Now I've ended up with a field with the name `j1::j0::a::i`. If I wanted to sub in mock data for `j1`, I'm out of luck - there's no way to name a field that way without creating it the way we did. I'd have to modify my script to not use the long name for `i`. And this is using one character names for relations - imagine what it would look like if we used descriptive names!

Let's take a look at the PigPen version:

    (let [a (pig/load-pig "numbers0.tsv" [i w])
          b (pig/load-pig "numbers1.tsv" [i x])
          c (pig/load-pig "numbers2.tsv" [i y])
          d (pig/load-pig "numbers3.tsv" [i z])

          j0 (pig/join (a on :i)
                       (b on :i)
                       merge)
          
          j1 (pig/join (j0 on :i)
                       (c on :i)
                       merge)
          
          j2 (pig/join (j1 on :i)
                       (d on :i)
                       merge)]
      (pig/dump j2))

_Note: `merge` is a Clojure function that takes any number of maps and merges them together._

When we run this, it returns the following data:

    {:i 1, :w 1, :x 2, :y 3, :z 4}

Simple, no? By using a consolidation function after each join, we get away from that nasty naming problem altogether.

<!--

### Dereferencing

(The next three sections need work, but I'm not sure how much of this part I want to keep anyway)

;; TODO Better description & example

Bad: Dereferencing vs field access semantics are confusing. Should be a syntax error, but is caught at runtime

Good: Dereferencing & field access is all done within clojure - usual clojure semantics

-->
<!--

### UDF & Inner Bags

;; TODO Better description & example

Bad: Sidecar files for anything interesting.
Types passed to sidecar are ambiguous.
Nested inner bags are alluring, but very hard to make work.

Good: Functions are inline - no extra files required, no other language, no interop issues.
Easy to do complex and simple work inline.

-->
<!--

### Unit tests

;; TODO Better description & example

Bad: To mock data, you have to store data to a file and define a new load with the exact schema

Good: Mock data is trivial - unit tests are possible
No schema redefinition to load intermediate data

-->
