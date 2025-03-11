# Rust Persistent Key-Value Store

This project is a simple implementation of a persistent, unordered key-value store in Rust.
This is intended as building block for applications that need (concurrent reads and writes,
low latency, durability) and their data fits into RAM. Basically a concurrent hashmap that
keeps its contents!

Under the hood, the store implements persistence via a write-ahead log that is periodically
compacted into snapshots. Where possible, it employs parallel reads/writes to make
best use of modern flash/SSD drives.

By default, past snapshots are kept but there is a CLI to prune snapshots down to a target
number of backups (automating this could be a future addition). Other ephemeral data, such
as failed snapshots or past write logs are automatically deleted once they are known to
have been superseeded by at least one valid snapshot.

## More details

See [main crate documentation](src/lib.rs)

## Getting Started

`cargo add persistent-kv`, then:

```rust
use persistent_kv::{Config, PersistentKeyValueStore};

// Create a new store. The specified folder will be used to store data.
let store: PersistentKeyValueStore<String, String> =
    PersistentKeyValueStore::new("/tmp/mystore",
        Config::default()).unwrap();

// Set some keys and retrieve some keys
store.set("foo", "is tired of foo").unwrap();
store.set("bar", "is not tired of bar").unwrap();
assert_eq!(store.get("foo"), Some("is tired of foo".to_string()));
assert_eq!(store.get("bar"), Some("is not tired of bar".to_string()));

// Delete the foo key
store.unset("foo").unwrap();
assert_eq!(store.get("foo"), None);

// Create a fresh store instance from the same data, bar is still there!
drop(store);
let store: PersistentKeyValueStore<String, String> =
    PersistentKeyValueStore::new("/tmp/mystore",
        Config::default()).unwrap();

assert_eq!(store.get("bar"), Some("is not tired of bar".to_string()));
```

There is a built-in API to use protobufs (via prost) on the value side:

```rust
use prost::Message;
use persistent_kv::{Config, PersistentKeyValueStore};

#[derive(Clone, PartialEq, Message)]
pub struct Foo {
    #[prost(uint32, tag = "1")]
    pub bar: u32,
}

let store: PersistentKeyValueStore<String, Foo> =
    PersistentKeyValueStore::new("/tmp/myprotostore",
        Config::default()).unwrap();
store.set_proto("foo", Foo {bar: 42}).unwrap();
assert_eq!(store.get_proto("foo").unwrap(), Some(Foo {bar: 42}));
```

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

