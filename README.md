# Rust Persistent Key-Value Store

This project is a simple implementation of a persistent, unordered key-value store in Rust.
This is intended as building block for applications that need all of _{concurrent reads and
writes, ultra low latency, high durability}_ and meet all of _{data fits into RAM, keys are
unordered, single process}_. Basically a concurrent hashmap that keeps its contents!

Under the hood, the store implements persistence via a write-ahead log that is periodically
compacted into snapshots. Where possible, it employs parallel reads/writes to make
best use of modern flash/SSD drives.

See [main crate documentation](src/lib.rs) for design goals, data management and performance tuning.

## Getting Started

### Adding the crate

```sh
cargo add persistent-kv
```

### Basic usage

```rust
use persistent_kv::{Config, PersistentKeyValueStore};

// Create a new store and write one key value pair to it, then drop the store.
let path = "tmp/mystore";
let store: PersistentKeyValueStore<String, String> =
    PersistentKeyValueStore::new(path, Config::default())?;
store.set("foo", "is here to stay")?;
drop(store);

// Create a fresh store instance and observe the key is still there.
let store: PersistentKeyValueStore<String, String> =
    PersistentKeyValueStore::new(path, Config::default())?;
store.get("foo") // Returns: Some("is here to stay")
```

By default, data is persisted immediately: if the process were to abort after the `set(key)` but before
the `drop(store)`, no data should be lost. However, only one instance can be active in any given folder at a time.

### Protobufs as value

There is a built-in API to use protobufs (via [prost](https://github.com/tokio-rs/prost)) on the value side:

```rust
use prost::Message;
use persistent_kv::{Config, PersistentKeyValueStore};

#[derive(Clone, PartialEq, Message)]
pub struct Foo {
    #[prost(uint32, tag = "1")]
    pub bar: u32,
}

let store: PersistentKeyValueStore<String, Foo> = ...
store.set_proto("foo", Foo {bar: 42})?;
store.get_proto("foo")?; // Returns: Some(Foo {bar: 42}))
```

### Configuration

See the [config module](src/config.rs) for available configuration options and the [main crate documentation](src/lib.rs) on notes how the defaults were derived.

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your discretion.