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
