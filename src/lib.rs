//! Unordered key-value store with all data held in memory but also persisted to disk for durability.
//!
//! The store is designed to be used as a building block for (distributed) systems that require
//! a high-throughput, low-latency, unordered key-value store with persistence guarantees.
//!
//! # Design goals
//!
//!  - Lightweight and with few dependencies
//!  - Support concurrent read/writes with minimal locking times
//!  - Tunable write throughput / persistence guarantee trade-off
//!  - Maximize block device throughput and exploit I/O parallelism if supported by OS and hardware
//!  - Amortized memory and disk usage is O(number of keys), <10% overhead over payload size (e.g.
//!    no spikes during snapshotting)
//!  - Support for both fixed-size and variable-size keys and values
//!
//! A good mental model is "Hashmap that keeps its contents between program runs". If more
//! advanced database features are required, [RocksDB](https://docs.rs/rocksdb/latest/rocksdb/)
//! or [SQLite](https://docs.rs/sqlite/latest/sqlite/) are a better choice.
//!
//! # Key and value format
//!
//! Both the key and the value side of the store operate exclusively on byte sequences,
//! converted from high level types via the [`crate::Serializable`] and [`crate::Deserializable`]
//! traits. This avoids deviation between in-memory representation of objects and their serialized
//! form, which could otherwise lead to subtle bugs. As a result of operating on serialized
//! data only, none of [`Send`] and [`Sync`] and none of the hash map traits [`Hash`] or
//! [`Eq`] are technically required for the key and value types.
//!
//! All fixed-size integer types, [`Vec<u8>`] and [`String`] are supported out of the box
//! as both keys and values. On the value side, [`prost::Message`] types are also directly
//! supported using the xxx_proto family of methods.
//!
//! # Implementation notes
//!
//! Persistence is implemented via a write-ahead log that is periodically compacted and
//! replaced by full snapshots. All files are stored in a single folder. Individual snapshots
//! can be internally sharded to ease parallel processing and keep file size reasonable.
//! See the [`snapshot_set`] module for details on snapshot format.
//!
//! The on-disk format is a series of records. Each record is a protobuf message prefixed
//! by a varint encoded length.
//!
//! # Performance notes
//!
//! If performance or throughput is a concern, you must benchmark and tune store configuration for
//! the exact hardware and OS you are targeting.
//!
//! Defaults in [`Config`] are ok as a starting point and were derived as follows:
//!
//!   1) Linux sees a 2-3x improvement in write throughput when using positioned writes (enabled)
//!   but the same setting has slightly negative effects on Windows (disabled).
//!
//!   2) No OS seems to benefit from sharding the write-ahead log (default is 1)
//!
//!   3) Target parallelism for snapshot reads/writes is limited by I/O controller concurrency which
//!   varies by device type (default is 8 which should suit most modern SSDs).
//!
//!   4) The number of memory buckets is never a huge factor, as a rule of thumb it should be
//!   above the number of simultaneous readers (default is 32)
mod config;
mod snapshot;
pub mod snapshot_set;
mod store;
mod types;

use std::{
    borrow::{Borrow, Cow},
    error::Error,
};

use snapshot_set::FileSnapshotSet;
use store::{FixedLengthKey64Bit, Store, StoreImpl, VariableLengthKey};

pub use config::Config;

pub struct PersistentKeyValueStore<K, V> {
    store: StoreImpl,
    phantom: std::marker::PhantomData<(K, V)>,
}

// We don't need K to be Sync + Send as we only operate on the serialized version
// when passing the data between threads internally and the external interface
// exposes only clones, not references.
unsafe impl<K, V> Sync for PersistentKeyValueStore<K, V> {}
unsafe impl<K, V> Send for PersistentKeyValueStore<K, V> {}

/// Trait for deserializing a type from a byte slice.
pub trait Deserializable {
    fn from_bytes(bytes: &[u8]) -> Self;
}

/// Trait for serializing a type to a byte slice or a fixed size byte array.
pub trait Serializable {
    const IS_FIXED_SIZE: bool;
    /// May return a borrowed slice or an owned vector.
    fn serialize(&self) -> Cow<'_, [u8]>;
    /// May return None if the type is not fixed size representable.
    fn serialize_fixed_size(&self) -> Option<[u8; 8]>;
}

impl<K, V> PersistentKeyValueStore<K, V>
where
    K: Serializable,
{
    /// Constructs a new store instance.
    /// The store will be backed by the given path and use the provided configuration.
    /// This function will block on restoring previously saved state from disk.
    ///
    /// # Example
    /// ```
    /// use persistent_kv::{Config, PersistentKeyValueStore};
    /// let store: PersistentKeyValueStore<String, String> =
    ///     PersistentKeyValueStore::new("/tmp/mystore1", Config::default()).unwrap();
    /// ```
    /// # Errors
    ///
    /// Propagates IO errors when reading from disk, also fails when the snapshot files
    /// don't follow the exact naming schema expected (and written) by this crate.
    pub fn new(path: impl AsRef<std::path::Path>, config: Config) -> Result<Self, Box<dyn Error>> {
        let snapshot_set = FileSnapshotSet::new(path.as_ref())?;
        Ok(Self {
            store: if <K as Serializable>::IS_FIXED_SIZE {
                StoreImpl::FixedKey(Store::new(snapshot_set, config)?)
            } else {
                StoreImpl::VariableKey(Store::new(snapshot_set, config)?)
            },
            phantom: std::marker::PhantomData,
        })
    }

    /// Removes a key from the store.
    /// Supports using borrowed keys (e.g. [`str`] for a [`String`] key).
    /// # Example
    /// ``` rust
    /// use persistent_kv::{Config, PersistentKeyValueStore};
    /// let store: PersistentKeyValueStore<String, String> =
    ///     PersistentKeyValueStore::new("/tmp/mystore2", Config::default()).unwrap();
    /// store.set("foo", "1").unwrap();
    /// assert_eq!(store.get("foo"), Some("1".to_string()));
    /// store.unset("foo").unwrap();
    /// assert_eq!(store.get("foo"), None);
    /// ```
    /// # Errors
    /// Propagates any IO errors that occur directly as a result of the write operation.
    pub fn unset<Q>(&self, key: &Q) -> Result<(), Box<dyn Error>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serializable,
    {
        match &self.store {
            StoreImpl::FixedKey(store) => store
                .unset(key.serialize_fixed_size().unwrap().borrow())
                .map(|_| ()),
            StoreImpl::VariableKey(store) => store.unset(key.serialize().borrow()).map(|_| ()),
        }
    }

    fn set_(&self, key: K, value: Vec<u8>) -> Result<(), Box<dyn Error>> {
        match &self.store {
            StoreImpl::FixedKey(store) => store
                .set(
                    FixedLengthKey64Bit(key.serialize_fixed_size().unwrap()),
                    value,
                )
                .map(|_| ()),
            StoreImpl::VariableKey(store) => store
                .set(VariableLengthKey(key.serialize().into_owned()), value)
                .map(|_| ()),
        }
    }

    fn get_<Q, F, V2>(&self, key: &Q, c: F) -> Option<V2>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serializable,
        F: FnOnce(Option<&[u8]>) -> Option<V2>,
    {
        match &self.store {
            StoreImpl::VariableKey(store) => store.get_convert(&key.serialize(), c),
            StoreImpl::FixedKey(store) => {
                store.get_convert(key.serialize_fixed_size().unwrap().borrow(), c)
            }
        }
    }
}

/// Store methods for simple values: Vec[u8], String, integers. We bypass all serialization
/// frameworks for these types.
impl<K, V> PersistentKeyValueStore<K, V>
where
    K: Serializable,
    V: Deserializable + Serializable,
{
    /// Sets a key-value pair in the store.
    /// If the key already exists, the value will be overwritten.
    /// # Example
    /// ```
    /// use persistent_kv::{Config, PersistentKeyValueStore};
    /// let store: PersistentKeyValueStore<String, String> =
    ///    PersistentKeyValueStore::new("/tmp/mystore3", Config::default()).unwrap();
    /// store.set("foo", "1").unwrap();
    /// assert_eq!(store.get("foo"), Some("1".to_string()));
    /// ```
    /// # Errors
    /// Propagates any IO errors that occur directly as a result of the write operation.
    pub fn set(&self, key: impl Into<K>, value: impl Into<V>) -> Result<(), Box<dyn Error>> {
        self.set_(key.into(), value.into().serialize().into_owned())
    }

    /// Retrieves a value from the store.
    /// Supports lookups using borrowed keys (e.g. [`str`] for a [`String`] key).
    /// # Example
    /// ``` rust
    /// use persistent_kv::{Config, PersistentKeyValueStore};
    /// let store: PersistentKeyValueStore<String, String> =
    ///    PersistentKeyValueStore::new("/tmp/mystore4", Config::default()).unwrap();
    /// store.set("foo", "1").unwrap();
    /// assert_eq!(store.get("foo"), Some("1".to_string()));
    /// ```
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serializable,
    {
        self.get_(key, |bytes| bytes.map(|bytes| V::from_bytes(bytes)))
    }
}

/// Store version for protobuf values.
impl<K, V> PersistentKeyValueStore<K, V>
where
    K: Serializable,
    V: prost::Message + Default,
{
    /// Sets a protobuf-coded value in the store.
    /// If the key already exists, the value will be overwritten.
    /// # Example
    /// ```
    /// use prost::Message;
    /// use persistent_kv::{Config, PersistentKeyValueStore};
    /// #[derive(Clone, PartialEq, Message)]
    /// pub struct Foo {
    ///     #[prost(uint32, tag = "1")]
    ///     pub bar: u32,
    /// }
    /// let store: PersistentKeyValueStore<String, Foo> =
    ///    PersistentKeyValueStore::new("/tmp/mystore5", Config::default()).unwrap();
    /// store.set_proto("foo", Foo {bar: 42}).unwrap();
    /// assert_eq!(store.get_proto("foo").unwrap(), Some(Foo {bar: 42}));
    /// ```
    /// # Errors
    /// Propagates any IO errors that occur directly as a result of the write operation.
    pub fn set_proto(
        &self,
        key: impl Into<K>,
        value: impl prost::Message,
    ) -> Result<(), Box<dyn Error>> {
        self.set_(key.into(), value.encode_to_vec())
    }

    /// Retrieves a protobuf-coded value from the store.
    /// Supports lookups using borrowed keys (e.g. [`str`] for a [`String`] key).
    /// # Example
    /// ```
    /// use prost::Message;
    /// use persistent_kv::{Config, PersistentKeyValueStore};
    /// #[derive(Clone, PartialEq, Message)]
    /// pub struct Foo {
    ///     #[prost(uint32, tag = "1")]
    ///     pub bar: u32,
    /// }
    /// let store: PersistentKeyValueStore<String, Foo> =
    ///    PersistentKeyValueStore::new("/tmp/mystore6", Config::default()).unwrap();
    /// store.set_proto("foo", Foo {bar: 42}).unwrap();
    /// assert_eq!(store.get_proto("foo").unwrap(), Some(Foo {bar: 42}));
    /// ```
    /// # Errors
    /// Forwards proto decode errors.
    pub fn get_proto<Q>(&self, key: &Q) -> Result<Option<V>, prost::DecodeError>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serializable,
    {
        self.get_(key, |bytes| bytes.map(|bytes| V::decode(bytes)))
            .transpose()
    }
}

/// Debug trait for PersistentKeyValueStore
impl<K, V> std::fmt::Debug for PersistentKeyValueStore<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (num_elements, num_bytes) = match &self.store {
            StoreImpl::FixedKey(store) => store.compute_size_info(),
            StoreImpl::VariableKey(store) => store.compute_size_info(),
        };
        write!(
            f,
            "PersistentKeyValueStore({} elements, {} KiB total size)",
            num_elements,
            num_bytes / 1024
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    // This set of tests is not exhaustive as the store itself is tested in-depth in the store module.
    // Below tests focus on the public API and the serialization/deserialization traits in as far
    // as the doctests don't already cover them.

    #[test]
    fn setget_string_string() {
        let tmp_dir = TempDir::new().unwrap();
        let store: PersistentKeyValueStore<String, String> =
            PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
        store.set("foo", "1").unwrap();
        assert_eq!(store.get("foo"), Some("1".to_string()));
        store.unset("foo").unwrap();
        assert_eq!(store.get("foo"), None);
    }

    #[test]
    fn setget_u64_u64() {
        let tmp_dir = TempDir::new().unwrap();
        let store: PersistentKeyValueStore<u64, u64> =
            PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
        store.set(35293853295u64, 1139131311u64).unwrap();
        assert_eq!(store.get(&35293853295u64), Some(1139131311u64));
        store.unset(&35293853295u64).unwrap();
        assert_eq!(store.get(&35293853295u64), None);
    }

    #[test]
    fn setget_i32_i32() {
        let tmp_dir = TempDir::new().unwrap();
        let store: PersistentKeyValueStore<i32, i32> =
            PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
        store.set(352938539, 113913131).unwrap();
        assert_eq!(store.get(&352938539), Some(113913131));
        store.unset(&352938539).unwrap();
        assert_eq!(store.get(&352938539), None);
    }

    #[test]
    fn debug_trait() {
        let tmp_dir = TempDir::new().unwrap();
        let store: PersistentKeyValueStore<String, String> =
            PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
        store.set("foo", "1".repeat(2048)).unwrap();
        assert_eq!(
            format!("{store:?}"),
            "PersistentKeyValueStore(1 elements, 2 KiB total size)"
        );
    }
}
