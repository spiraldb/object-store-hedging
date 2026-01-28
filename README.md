# Hedged Object Store

[![Crates.io](https://img.shields.io/crates/v/object-store-hedging.svg)](https://crates.io/crates/object-store-hedging)

This crate extends the [`object-store`] to include request hedging for read requests.

Hedging is a common technique for reducing tail latencies in distributed systems, as explored in papers such as [The Tail at Scale] and [AnyBlob].

## Usage

```rust
use object_store::{ObjectStore, path::Path};
use object_store_hedging::{HedgedStore, HedgingConfig};

// Wrap any ObjectStore with hedging
let store = object_store::aws::AmazonS3Builder::from_env()
    .with_bucket_name("my-bucket")
    .build()?;
let store = HedgedStore::new(store, HedgingConfig::default());

// Use as normal - hedging happens automatically on reads
let data = store.get(&Path::from("my-key")).await?;
```

[`object-store`]: https://github.com/apache/arrow-rs-object-store
[The Tail at Scale]: https://research.google/pubs/the-tail-at-scale
[AnyBlob]: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf
