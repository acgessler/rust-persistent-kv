//! Persistent key value store.
//!
//! Persistence is achieved via
//!  - full snapshots that are periodically written to disk
//!  - write-ahead log (WAL) to capture recent additions

use std::{ borrow::{ Borrow, Cow }, error::Error, str };

use store::{ FixedLengthKey, VariableLengthKey };

mod store;
mod config;
mod snapshots;
mod snapshot_set;

// Hardcoded underlying implementations of store to keep generic interface small.
enum StoreImpl {
    FixedKey(store::PersistentRawKeyValueStore<store::FixedLengthKey>),
    VariableKey(store::PersistentRawKeyValueStore<store::VariableLengthKey>),
}

pub struct PersistentKeyValueStore<K, V> {
    store: StoreImpl,
    phantom: std::marker::PhantomData<(K, V)>,
}

pub trait SerializableValue {
    fn from_bytes(bytes: &[u8]) -> Self;
}
pub trait SerializableKey {
    const IS_FIXED_SIZE: bool;
    fn to_bytes_ref(&self) -> Cow<'_, [u8]>;
    fn to_fixed_size(&self) -> Option<[u8; 8]>;
}

impl<K, V> PersistentKeyValueStore<K, V>
    where K: SerializableKey, V: SerializableValue + SerializableKey
{
    pub fn new(path: &std::path::Path, config: config::Config) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            store: if <K as SerializableKey>::IS_FIXED_SIZE {
                StoreImpl::FixedKey(store::PersistentRawKeyValueStore::new(path, config)?)
            } else {
                StoreImpl::VariableKey(store::PersistentRawKeyValueStore::new(path, config)?)
            },
            phantom: std::marker::PhantomData,
        })
    }

    pub fn set(&self, key: impl Into<K>, value: impl Into<V>) -> &Self {
        let key = key.into();
        let value = value.into().to_bytes_ref().into_owned(); //  TODO: Serde
        match &self.store {
            StoreImpl::FixedKey(store) => {
                store.set(FixedLengthKey(key.to_fixed_size().unwrap()), value);
            }
            StoreImpl::VariableKey(store) => {
                store.set(VariableLengthKey(key.to_bytes_ref().into_owned()), value);
            }
        }
        self
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V> where K: Borrow<Q>, Q: ?Sized + SerializableKey {
        (
            match &self.store {
                StoreImpl::FixedKey(store) => { store.get(key.to_fixed_size().unwrap().borrow()) }
                StoreImpl::VariableKey(store) => { store.get(key.to_bytes_ref().borrow()) }
            }
        ).map(|v| V::from_bytes(&v))
    }

    pub fn unset<Q>(&self, key: &Q) -> &Self where K: Borrow<Q>, Q: ?Sized + SerializableKey {
        match &self.store {
            StoreImpl::FixedKey(store) => {
                store.unset(key.to_fixed_size().unwrap().borrow());
            }
            StoreImpl::VariableKey(store) => {
                store.unset(key.to_bytes_ref().borrow());
            }
        }
        self
    }
}

impl SerializableKey for String {
    const IS_FIXED_SIZE: bool = false;
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
    fn to_fixed_size(&self) -> Option<[u8; 8]> {
        None
    }
}

impl SerializableValue for String {
    fn from_bytes(bytes: &[u8]) -> Self {
        str::from_utf8(bytes).unwrap().to_string()
    }
}

impl SerializableKey for str {
    const IS_FIXED_SIZE: bool = false;
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
    fn to_fixed_size(&self) -> Option<[u8; 8]> {
        None
    }
}

impl SerializableValue for u64 {
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut buf = [0; 8];
        buf.copy_from_slice(&bytes[..8]);
        u64::from_le_bytes(buf)
    }
}

impl SerializableKey for u64 {
    const IS_FIXED_SIZE: bool = true;
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Owned(self.to_le_bytes().to_vec())
    }
    fn to_fixed_size(&self) -> Option<[u8; 8]> {
        Some(self.to_le_bytes())
    }
}

impl SerializableKey for u32 {
    const IS_FIXED_SIZE: bool = true;
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Owned(self.to_le_bytes().to_vec())
    }
    fn to_fixed_size(&self) -> Option<[u8; 8]> {
        let mut buf = [0; 8];
        buf.copy_from_slice(&self.to_le_bytes()[..]);
        Some(buf)
    }
}

impl SerializableKey for u16 {
    const IS_FIXED_SIZE: bool = true;
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Owned(self.to_le_bytes().to_vec())
    }
    fn to_fixed_size(&self) -> Option<[u8; 8]> {
        let mut buf = [0; 8];
        buf.copy_from_slice(&self.to_le_bytes()[..]);
        Some(buf)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use super::*;

    #[test]
    fn setget_string_string() {
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

    #[test]
    fn setget_int_int() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let store: PersistentKeyValueStore<u64, u64> = PersistentKeyValueStore::new(
                tmp_dir.path(),
                config::Config::default()
            ).unwrap();
            store.set(35293853295u64, 1139131311u64);
            assert_eq!(store.get(&35293853295u64), Some(1139131311u64));
        }
    }
}
