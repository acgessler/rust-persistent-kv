//! Persistent key value store.
//!
//! Persistence is achieved via
//!  - full snapshots that are periodically written to disk
//!  - write-ahead log (WAL) to capture recent additions

use std::{ error::Error, str };

mod store;
mod config;
mod snapshots;
mod snapshot_set;

pub struct PersistentKeyValueStore<K, V> {
    store: store::PersistentRawKeyValueStore,
    phantom_k: std::marker::PhantomData<K>,
    phantom_v: std::marker::PhantomData<V>,
}

pub trait Deserializable {
    fn from_bytes(bytes: &[u8]) -> Self;
}

pub trait Serializable {
    fn to_bytes(self) -> Vec<u8>;
}

impl<K, V> PersistentKeyValueStore<K, V> {
    pub fn new(path: &std::path::Path, config: config::Config) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            store: store::PersistentRawKeyValueStore::new(path, config)?,
            phantom_k: std::marker::PhantomData,
            phantom_v: std::marker::PhantomData,
        })
    }
}

impl<K, V> PersistentKeyValueStore<K, V>
    where K: Serializable + Deserializable, V: Serializable + Deserializable
{
    pub fn get(&self, key: impl Into<K>) -> Option<V> {
        self.store.get(&key.into().to_bytes()).map(|v| V::from_bytes(&v))
    }

    pub fn set(&self, key: impl Into<K>, value: impl Into<V>) -> &Self {
        self.store.set(key.into().to_bytes(), value.into().to_bytes());
        self
    }

    pub fn unset(&self, key: impl Into<K>) -> &Self {
        self.store.unset(&key.into().to_bytes());
        self
    }
}

impl Deserializable for String {
    fn from_bytes(bytes: &[u8]) -> Self {
        str::from_utf8(bytes).unwrap().to_string()
    }
}
impl Serializable for String {
    fn to_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use super::*;

    #[test]
    fn setget_string() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let store: PersistentKeyValueStore<String, String> = PersistentKeyValueStore::new(
                tmp_dir.path(),
                config::Config::default()
            ).unwrap();
            store.set("foo", "1");
            assert_eq!(store.get("foo"), Some("1".to_string()));
        }
    }
}
