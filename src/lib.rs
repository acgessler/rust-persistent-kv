//! Persistent key value store.
//!
//! Persistence is achieved via
//!  - full snapshots that are periodically written to disk
//!  - write-ahead log (WAL) to capture recent additions

use std::error::Error;


mod store;
mod config;
mod snapshots;
mod snapshot_set;

pub struct PersistentKeyValueStore<K, V> where K : Into<String>,
{
    store: store::PersistentRawKeyValueStore,
    phantom_k: std::marker::PhantomData<K>,
    phantom_v: std::marker::PhantomData<V>,
}

impl<K, V> PersistentKeyValueStore<K, V> where K : Into<String>,
{
    pub fn new(path: &std::path::Path, config: config::Config) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            store: store::PersistentRawKeyValueStore::new(path, config)?,
            phantom_k: std::marker::PhantomData,
            phantom_v: std::marker::PhantomData,
        })
    }
}

