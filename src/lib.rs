//! Persistent key value store.
//!
//! Persistence is achieved via
//!  - full snapshots that are periodically written to disk
//!  - write-ahead log (WAL) to capture recent additions

use std::{ borrow::{ Borrow, Cow }, error::Error, str };

use store::{ FixedLengthKey64Bit, VariableLengthKey, Store, StoreImpl };

pub use config::Config;

mod store;
mod config;
mod snapshots;
mod snapshot_set;

pub struct PersistentKeyValueStore<K, V> {
    store: StoreImpl,
    phantom: std::marker::PhantomData<(K, V)>,
}

pub trait SerializableValue {
    fn from_bytes(bytes: &[u8]) -> Self;
}
pub trait SerializableKey {
    const IS_FIXED_SIZE: bool;
    fn serialize(&self) -> Cow<'_, [u8]>;
    fn serialize_fixed_size(&self) -> Option<[u8; 8]>;
}

impl<K, V> PersistentKeyValueStore<K, V>
    where K: SerializableKey, V: SerializableValue + SerializableKey
{
    pub fn new(path: &std::path::Path, config: Config) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            store: if <K as SerializableKey>::IS_FIXED_SIZE {
                StoreImpl::FixedKey(Store::new(path, config)?)
            } else {
                StoreImpl::VariableKey(Store::new(path, config)?)
            },
            phantom: std::marker::PhantomData,
        })
    }

    pub fn set(&self, key: impl Into<K>, value: impl Into<V>) {
        let key = key.into();
        let value = value.into().serialize().into_owned(); //  TODO: Serde
        match &self.store {
            StoreImpl::FixedKey(store) => {
                store.set(FixedLengthKey64Bit(key.serialize_fixed_size().unwrap()), value);
            }
            StoreImpl::VariableKey(store) => {
                store.set(VariableLengthKey(key.serialize().into_owned()), value);
            }
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V> where K: Borrow<Q>, Q: ?Sized + SerializableKey {
        let value = match &self.store {
            StoreImpl::FixedKey(store) => {
                store.get(key.serialize_fixed_size().unwrap().borrow())
            }
            StoreImpl::VariableKey(store) => { store.get(&key.serialize()) }
        };
        value.map(|value| V::from_bytes(&value))
    }

    pub fn unset<Q>(&self, key: &Q) where K: Borrow<Q>, Q: ?Sized + SerializableKey {
        match &self.store {
            StoreImpl::FixedKey(store) => {
                store.unset(key.serialize_fixed_size().unwrap().borrow());
            }
            StoreImpl::VariableKey(store) => {
                store.unset(key.serialize().borrow());
            }
        }
    }
}

impl SerializableKey for String {
    const IS_FIXED_SIZE: bool = false;
    fn serialize(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
    fn serialize_fixed_size(&self) -> Option<[u8; 8]> {
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
    fn serialize(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
    fn serialize_fixed_size(&self) -> Option<[u8; 8]> {
        None
    }
}

macro_rules! implement_integer_key_type {
    ($integer_type:ident) => {
        impl SerializableValue for $integer_type {
            fn from_bytes(bytes: &[u8]) -> Self {
                let mut buf = [0; std::mem::size_of::<$integer_type>()];
                buf.copy_from_slice(&bytes[..std::mem::size_of::<$integer_type>()]);
                $integer_type::from_le_bytes(buf)
            }
        }
        
        impl SerializableKey for $integer_type {
            const IS_FIXED_SIZE: bool = true;
            fn serialize(&self) -> Cow<'_, [u8]> {
                Cow::Owned(self.to_le_bytes().to_vec())
            }
            fn serialize_fixed_size(&self) -> Option<[u8; 8]> {
                let mut buf = [0; 8];
                buf.copy_from_slice(&self.to_le_bytes()[..]);
                Some(buf)
            }
        }
    };
}

implement_integer_key_type!(u64);
implement_integer_key_type!(i64);
implement_integer_key_type!(u32);
implement_integer_key_type!(i32);
implement_integer_key_type!(u16);
implement_integer_key_type!(i16);
implement_integer_key_type!(u8);
implement_integer_key_type!(i8);

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use super::*;
    // This set of tests focuses on the interface with different key, value types,
    // tests for the actual persistence behaviour are in store.ts.

    #[test]
    fn setget_string_string() {
        let tmp_dir = TempDir::new().unwrap();
        let store: PersistentKeyValueStore<String, String> = PersistentKeyValueStore::new(
            tmp_dir.path(),
            Config::default()
        ).unwrap();
        store.set("foo", "1");
        assert_eq!(store.get("foo"), Some("1".to_string()));
    }

    #[test]
    fn setget_int_int() {
        let tmp_dir = TempDir::new().unwrap();
        let store: PersistentKeyValueStore<u64, u64> = PersistentKeyValueStore::new(
            tmp_dir.path(),
            Config::default()
        ).unwrap();
        store.set(35293853295u64, 1139131311u64);
        assert_eq!(store.get(&35293853295u64), Some(1139131311u64));
    }
}
