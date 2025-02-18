//! Persistent key value store where both key and value are strings.
//!
//! All modifications are assigned a unique monotonically increasing ID.
//!
//! Persistence is achieved via
//!  - full snapshots that are periodically written to disk
//!  - write-ahead log to capture recent additions.
//!
//! Both are identical on-disk formats except that snapshots contain unique
//! key-value pairs only but write logs may contain multiple entries
//! for the same key.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    hash::{ DefaultHasher, Hash, Hasher },
    io::Write,
    sync::{ atomic::AtomicU64, Arc, Mutex, RwLock },
};

struct Bucket {
    data: Arc<RwLock<HashMap<String, String>>>,
}

pub struct PersistentKeyValueStore {
    bucket_count: u32,
    data: Vec<Bucket>,

    wal: Mutex<SnapshotWriter>,
}

struct SnapshotWriter {
    file: File,
}

impl SnapshotWriter {
    fn new() -> Self {
        Self {
            file: OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("write-ahead-log.txt")
            .unwrap(),
        }
    }

    fn append_entry(&mut self, key: &str, value: Option<&str>) {
        // TODO(acgessler): non text format and no character limitations
        let entry = format!("{}\t{}\n", key, value.unwrap_or("")); 
        self.file.write(entry.as_bytes()).unwrap();
        // TODO(acgessler): Offer config option as to whether F_FULLFSYNC is required
        // or if loosing a few writes in a power-off scenario is acceptable.
        self.file.sync_all().unwrap();
    }
}

impl PersistentKeyValueStore {
    const DEFAULT_BUCKET_COUNT: u32 = 16;
    pub fn new(bucket_count: Option<u32>) -> Self {
        let bucket_count = bucket_count.unwrap_or(PersistentKeyValueStore::DEFAULT_BUCKET_COUNT);
        let mut data = Vec::with_capacity(bucket_count as usize);
        for _ in 0..bucket_count {
            data.push(Bucket {
                data: RwLock::new(HashMap::new()).into(),
            });
        }
        Self { bucket_count, data, wal: Mutex::new(SnapshotWriter::new()) }
    }

    pub fn set(&self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        let bucket = self.get_bucket(&key);

        // Hold lock on WAL throughout entire write operation to ensure that the
        // write-ahead log is consistently ordered.
        let mut wal = self.wal.lock().unwrap();
        wal.append_entry(&key, Some(&value));

        // Hold shorter lock on in-memory bucket to avoid blocking readers.
        let mut data = bucket.data.write().unwrap();
        data.insert(key, value);
    }

    pub fn unset(&self, key: impl Into<String>) {
        let key = key.into();
        let bucket = self.get_bucket(&key);

        // See notes on lock usage in set()
        let mut wal = self.wal.lock().unwrap();
        wal.append_entry(&key, None);
        let mut data = bucket.data.write().unwrap();
        data.remove(&key);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let bucket = self.get_bucket(key);
        bucket.data.read().unwrap().get(key).cloned()
    }

    fn get_bucket(&self, key: &str) -> &Bucket {
        let hash = PersistentKeyValueStore::hash(key);
        let index = hash % (self.bucket_count as u64);
        &self.data[index as usize]
    }

    fn hash(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_setget() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        store.set("bar", "2");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        assert_eq!(store.get("bar"), Some(String::from("2")));
    }
    #[test]
    fn basic_get_nonexisting() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        assert_eq!(store.get("bar"), None);
    }
    #[test]
    fn basic_set_overwrite() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        store.set("bar", "2");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        assert_eq!(store.get("bar"), Some(String::from("2")));
        store.set("bar", "3");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        assert_eq!(store.get("bar"), Some(String::from("3")));
    }
    #[test]
    fn basic_unset() {
        let store = PersistentKeyValueStore::new(None);
        store.set("foo", "1");
        assert_eq!(store.get("foo"), Some(String::from("1")));
        store.unset("foo");
        assert_eq!(store.get("foo"), None);
    }
}
