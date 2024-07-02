scala:
- [ ] Use Immutable data structures for MrdtSet, MrdtList, MrdtQueue (see rust implementation for reference)
- [ ] Write some micro benchmarks to compare the MRDT implementation to the CRDT implementations
- [ ] How can we speed up the merge function of sets? Use red black trees?
- [ ] (maybe) add some more MRDT data structures (e.g. MrdtMap, MrdtBinaryTree, MrdtGraph, etc.)
- [ ] Proper docs

rust:
- [ ] Implement diffing for the quark store
  - [ ] Measure storage impact (no-diff, gzip, diff, gzip+diff)
- [ ] Optimize the mrdt implementation (lots of low hanging fruit, lot's of clones all over the place)
- [ ] Add real world example and add some benchmarks for that
- [ ] Proper docs
- [ ] (maybe) add some more MRDT data structures (e.g. MrdtMap, MrdtBinaryTree, MrdtGraph, etc.)
