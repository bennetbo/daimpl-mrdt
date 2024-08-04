- How to make it run with Scala3 and/or Scala2.13 (cross-build?)
  - Every library that we found uses the ClassTag feature, which the Scala3 compiler doesn't support
  - https://github.com/datastax/spark-cassandra-connector/tree/master
  - Seems easier to do with other languages (e.g- Rust, Go, ...)
- How do other replicas know with which version to merge?
  - Establish a "main" replica, that we merge into
  - Application level concern?
- Diffing + Block Store
  - How do we select the right entries between the lca and the current commit id so we can compute the diff?
    - Can we use some kind of select with a range over versions (vector clocks)?
  - Efficient insertion
- What's the end goal? Benchmarking the MRDT implementation in comparison to the existing CRDT implementations?
  - CRDT does not use same data store, therefore hard to compare

- Idea:
  - Micro-benchmark scala implementation CRDTs/MRDTs
  - Benchmark whole Quark Implementation and see how it performs, what happens, ...
  - JMH Scala3 Benchmark

---

- How do they decide which datatype corresponds to which commit? Can you only have one datatype?

---
Meet Jul 11

Status & open questions:
- We have a (partially) working implementation of the MRDTs + Quark store in Rust
- Immutable data structures (e.g. RBHashSet) seems to perform much worse in Rust (see benchmark),
  therefore we decided to go with the default Rust HashSet implementation using a non-cryptographic hash function
- We did a performance evaluation of the document benchmark they do in the paper
  -> Problem: Ord Set merges are super slow (>100ms for merging sets with ~1000 elements)
  - How can we improve performance of the Ord Set merge?
  - Currently we do:
    - Store the index of each element in a hashmap
    - When merging, we iterate over the elements and construct the "mathematical representation" of the set
      e.g. [("a", 0), ("b", 1), ("c", 2)] -> [("a", "b"), ("a", "c"), ("b", "c")]
    - We perform the merge operation as described in the paper
      - Intersection of lca, left, right + diff of lca and left + diff of lca and right
    - Then we perform a graph search to reconstruct the hashmap
- How does the diffing actually work? At this point we are questioning whether the "Quark store" was actually ever implemented (at least not with Scylla)?
  - Looking at the code, the benchmark for measuring diffing is actually using git directly
    See paper "(computed by Quark’s content-addressable abstraction)"
  - Also found some interesting code in the authors github repo: /ocaml-irmin/apps/tpcc/irbmap.ml
    - Maybe an alternative implementation of the quark store is here?
      /ocaml-irmin/red_black_tree/mem/irbset.ml
  - Do we need to take another approach (e.g. using a different data store)?

TODO:
- Ob set immutable set faster?
- Construct Ob sets early and do set merge only
- Find out how merging works in Ocaml implementation
