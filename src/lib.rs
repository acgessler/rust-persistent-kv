//! Persistent key value store where both key and value are strings.
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
    hash::{ DefaultHasher, Hash, Hasher },
    path::Path,
    sync::{ Arc, Mutex, RwLock },
};

mod snapshots;

struct Bucket {
    data: Arc<RwLock<HashMap<String, String>>>,
}

pub struct PersistentKeyValueStore {
    bucket_count: u32,
    data: Vec<Bucket>,
    wal: Mutex<snapshots::SnapshotWriter>,
}

impl PersistentKeyValueStore {
    const DEFAULT_BUCKET_COUNT: u32 = 16;
    pub fn new(path: &Path, bucket_count: Option<u32>) -> Self {
        let bucket_count = bucket_count.unwrap_or(PersistentKeyValueStore::DEFAULT_BUCKET_COUNT);
        let mut data = Vec::with_capacity(bucket_count as usize);
        for _ in 0..bucket_count {
            data.push(Bucket {
                data: RwLock::new(HashMap::new()).into(),
            });
        }
        let wal_path = path.join("wal.bin");
        let self_ = Self {
            bucket_count,
            data,
            wal: Mutex::new(snapshots::SnapshotWriter::new(&wal_path, true)),
        };
        snapshots::SnapshotReader
            ::new(&wal_path)
            .read_entries(|entry| {
                let bucket = self_.get_bucket(&entry.key);
                let mut data = bucket.data.write().unwrap();

                if entry.value.is_empty() {
                    data.remove(&entry.key);
                } else {
                    data.insert(entry.key, String::from_utf8(entry.value).unwrap());
                }
            })
            .expect("Failed to read write-ahead log");
        self_
    }

    pub fn set(&self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        let bucket = self.get_bucket(&key);

        // Hold lock on WAL throughout entire write operation to ensure that the
        // write-ahead log is consistently ordered.
        let mut wal = self.wal.lock().unwrap();
        wal.append_entry(Self::make_snapshot_entry(&key, Some(&value))).expect(
            "Failed to write set op to write-ahead log"
        );

        // Hold shorter lock on in-memory bucket to avoid blocking concurrent
        // readers on the full I/O operation.
        let mut data = bucket.data.write().unwrap();
        data.insert(key, value);
    }

    pub fn unset(&self, key: impl Into<String>) {
        let key = key.into();
        let bucket = self.get_bucket(&key);

        // See notes on lock usage in set()
        let mut wal = self.wal.lock().unwrap();
        wal.append_entry(Self::make_snapshot_entry(&key, None)).expect(
            "Failed to write unset op to write-ahead log"
        );
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

    fn make_snapshot_entry(key: &str, value: Option<&str>) -> snapshots::SnapshotEntry {
        snapshots::SnapshotEntry {
            key: key.to_string(),
            value: value.map(|v| v.as_bytes().to_vec()).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn basic_setget() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let store = PersistentKeyValueStore::new(tmp_dir.path(), None);
            store.set("foo", "1");
            store.set("bar", "2");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("2")));
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None);
            assert_eq!(store2.get("foo"), Some(String::from("1")));
            assert_eq!(store2.get("bar"), Some(String::from("2")));
        }
    }

    #[test]
    fn basic_get_nonexisting() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let store = PersistentKeyValueStore::new(tmp_dir.path(), None);
            store.set("foo", "1");
            assert_eq!(store.get("bar"), None);
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None);
            assert_eq!(store2.get("bar"), None);
        }
    }

    #[test]
    fn basic_set_overwrite() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let store = PersistentKeyValueStore::new(tmp_dir.path(), None);
            store.set("foo", "1");
            store.set("bar", "2");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("2")));
            store.set("bar", "3");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("3")));
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None);
            assert_eq!(store2.get("foo"), Some(String::from("1")));
            assert_eq!(store2.get("bar"), Some(String::from("3")));
        }
    }

    #[test]
    fn basic_unset() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let store = PersistentKeyValueStore::new(tmp_dir.path(), None);
            store.set("foo", "1");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            store.unset("foo");
            assert_eq!(store.get("foo"), None);
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None);
            assert_eq!(store2.get("foo"), None);
        }
    }
}
