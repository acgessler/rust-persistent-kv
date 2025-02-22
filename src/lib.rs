//! Persistent key value store where both key and value are strings.
//!
//! Persistence is achieved via
//!  - full snapshots that are periodically written to disk
//!  - write-ahead log (WAL) to capture recent additions
//!
//! Both are identical on-disk formats except that snapshots contain unique
//! key-value pairs only but WAL may contain repeat entries.
use std::{
    collections::HashMap,
    error::Error,
    hash::{ DefaultHasher, Hash, Hasher },
    path::Path,
    sync::{ atomic::AtomicU64, Arc, Mutex, MutexGuard, RwLock },
};

use snapshot_set::{ SnapshotInfo, SnapshotType };

mod snapshots;
mod snapshot_set;

struct Bucket {
    data: Arc<RwLock<HashMap<String, String>>>,
}

pub struct PersistentKeyValueStore {
    bucket_count: u32,
    data: Vec<Bucket>, // TODO: cache
    wal: Arc<Mutex<snapshots::SnapshotWriter>>,
    snapshot_set: Arc<Mutex<snapshot_set::SnapshotSet>>,
    update_counter: AtomicU64,
    full_snapshot_writer_thread: Option<std::thread::JoinHandle<()>>,
    full_snapshot_writer_sender: Option<std::sync::mpsc::Sender<SnapshotTask>>,
}

struct SnapshotTask {
    data_copy: HashMap<String, String>,
    snapshot: SnapshotInfo,
}

impl Drop for PersistentKeyValueStore {
    fn drop(&mut self) {
        self.full_snapshot_writer_sender = None;
        self.full_snapshot_writer_thread.take().unwrap().join().unwrap();
    }
}

impl PersistentKeyValueStore {
    const DEFAULT_BUCKET_COUNT: u32 = 16;
    const UPDATE_THRESHOLD: u64 = 1000; // TODO(acgessler): make configurable

    pub fn new(path: &Path, bucket_count: Option<u32>) -> Result<Self, Box<dyn Error>> {
        let bucket_count = bucket_count.unwrap_or(PersistentKeyValueStore::DEFAULT_BUCKET_COUNT);
        let mut data = Vec::with_capacity(bucket_count as usize);
        for _ in 0..bucket_count {
            data.push(Bucket {
                data: RwLock::new(HashMap::new()).into(),
            });
        }
        let mut snapshot_set = snapshot_set::SnapshotSet::new(path)?;

        // Construct instance, imbuing it with a write-ahead log to persist new entries.
        let wal_path = &snapshot_set.create_or_get_snapshot(SnapshotType::Diff, true)?;
        let mut self_ = Self {
            bucket_count,
            data,
            snapshot_set: Arc::new(Mutex::new(snapshot_set)),
            wal: Mutex::new(snapshots::SnapshotWriter::new(&wal_path.path, true)).into(),
            full_snapshot_writer_thread: None,
            full_snapshot_writer_sender: None,
            update_counter: AtomicU64::new(0),
        };

        // Restore state from past snapshots. This will read the last full snapshot,
        // if available, and then replay all write-ahead log entries since that snapshot.
        {
            let snapshot_set = self_.snapshot_set.lock().unwrap();
            let snapshots_to_restore = snapshot_set.get_snapshots_to_restore();
            for snapshot_path in snapshots_to_restore {
                snapshots::SnapshotReader
                    ::new(&snapshot_path.path)
                    .read_entries(|entry| {
                        let bucket = self_.get_bucket(&entry.key);
                        let mut data = bucket.data.write().unwrap();

                        if entry.value.is_empty() {
                            data.remove(&entry.key);
                        } else {
                            data.insert(entry.key, String::from_utf8(entry.value).unwrap());
                        }
                    })
                    .expect(
                        format!(
                            "Failed to read write-ahead log snapshot: {:?}",
                            snapshot_path
                        ).as_str()
                    );
            }
        }

        // Start a thread that periodically writes full snapshots to disk.
        let (full_snapshot_writer_sender, full_snapshot_writer_receiver) =
            std::sync::mpsc::channel();
        let snapshot_set = self_.snapshot_set.clone();
        self_.full_snapshot_writer_sender = Some(full_snapshot_writer_sender);
        self_.full_snapshot_writer_thread = Some(
            std::thread::spawn(move || {
                loop {
                    match full_snapshot_writer_receiver.recv() {
                        Err(_) => {
                            break;
                        }
                        Ok(mut snapshot_task) => {
                            // Skip over any queued snapshots that we're not keeping up with and process the latest only.
                            while
                                let Ok(queued_snapshot_task) =
                                    full_snapshot_writer_receiver.try_recv()
                            {
                                snapshot_task = queued_snapshot_task;
                            }
                            let mut writer = snapshots::SnapshotWriter::new(
                                &snapshot_task.snapshot.path,
                                false
                            );
                            for (key, value) in snapshot_task.data_copy.iter() {
                                writer
                                    .append_entry(Self::make_snapshot_entry(key, Some(value)))
                                    .expect("Failed to write full snapshot entry to disk");
                            }
                            snapshot_set.lock().unwrap().publish_completed_snapshot(
                                snapshot_task.snapshot.ordinal
                            ).unwrap();
                            // TODO(acgessler): prune now obsolete write ahead logs.
                        }
                    }
                }
            })
        );

        Ok(self_)
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();

        // Hold lock on WAL throughout entire write operation to ensure that the
        // write-ahead log is consistently ordered.
        let mut wal = self.wal.lock().unwrap();
        wal.append_entry(Self::make_snapshot_entry(&key, Some(&value))).expect(
            "Failed to write set op to write-ahead log"
        );
        self.maybe_trigger_full_snapshot(&mut wal);

        // Hold shorter lock on in-memory bucket to avoid blocking concurrent
        // readers on the full I/O operation.
        let bucket = self.get_bucket(&key);
        let mut data = bucket.data.write().unwrap();
        data.insert(key, value);
    }

    pub fn unset(&mut self, key: impl Into<String>) {
        let key = key.into();
        let bucket = self.get_bucket(&key);

        // See notes on lock usage in set()
        let mut wal = self.wal.lock().unwrap();
        wal.append_entry(Self::make_snapshot_entry(&key, None)).expect(
            "Failed to write unset op to write-ahead log"
        );
        self.maybe_trigger_full_snapshot(&mut wal);
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

    fn maybe_trigger_full_snapshot(&self, wal: &mut MutexGuard<snapshots::SnapshotWriter>) {
        // If reaching update threshold, trigger full snapshot write.
        // - Allocate the snapshot immediately and make a copy of all data.
        // - Instantly switch to a new WAL.
        // - Signal the offline thread to carry out the snapshot.
        if
            (self.update_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1) %
                Self::UPDATE_THRESHOLD == 0
        {
            let mut snapshot_set = self.snapshot_set.lock().unwrap();
            let pending_snapshot = snapshot_set
                .create_or_get_snapshot(SnapshotType::Pending, false)
                .unwrap();
            **wal = snapshots::SnapshotWriter::new(
                &snapshot_set.create_or_get_snapshot(SnapshotType::Diff, false).unwrap().path,
                true
            );
            let mut data_copy = HashMap::new();
            for bucket in self.data.iter() {
                let data = bucket.data.read().unwrap();
                for (key, value) in data.iter() {
                    data_copy.insert(key.clone(), value.clone());
                }
            }
            self.full_snapshot_writer_sender
                .as_ref()
                .unwrap()
                .send(SnapshotTask { data_copy, snapshot: pending_snapshot.clone() })
                .unwrap();
        }
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
            let mut store = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            store.set("foo", "1");
            store.set("bar", "2");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("2")));
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            assert_eq!(store2.get("foo"), Some(String::from("1")));
            assert_eq!(store2.get("bar"), Some(String::from("2")));
        }
    }

    #[test]
    fn basic_get_nonexisting() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            store.set("foo", "1");
            assert_eq!(store.get("bar"), None);
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            assert_eq!(store2.get("bar"), None);
        }
    }

    #[test]
    fn basic_set_overwrite() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            store.set("foo", "1");
            store.set("bar", "2");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("2")));
            store.set("bar", "3");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("3")));
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            assert_eq!(store2.get("foo"), Some(String::from("1")));
            assert_eq!(store2.get("bar"), Some(String::from("3")));
        }
    }

    #[test]
    fn basic_unset() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            store.set("foo", "1");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            store.unset("foo");
            assert_eq!(store.get("foo"), None);
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), None).unwrap();
            assert_eq!(store2.get("foo"), None);
        }
    }
}
