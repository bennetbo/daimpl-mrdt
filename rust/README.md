# DAIMPL-MRDT

### Setup

To build the project execute the following command:

```bash
cargo build
```

To run the tests execute the following command:

```bash
cargo test
```

To run a basic example using the quark store you need to setup scylla by running:

```bash
scripts/setup_db.sh
```

Then you can run the example by executing:

```bash
cargo run --bin mrdt_rs
```
