# DAIMPL-MRDT

We tried to replicate and evaluate an alternative approach to CRDTs called [MRDTs](https://dl.acm.org/doi/pdf/10.1145/3360580).

A final report can be found [here](https://github.com/bennetbo/daimpl-mrdt/blob/main/report.pdf).

The repository consists of the following projects:

- scala: A scala implementation of various MRDT data structures
- rust: A rust implementation of the MRDT data structures and an implementation of the Quark store using scylla-db as the underlying storage engine
