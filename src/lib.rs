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

use config::Config;
use snapshot_set::{ SnapshotInfo, SnapshotSet, SnapshotType };

mod snapshots;
mod snapshot_set;
mod config;

struct Bucket {
    data: Arc<RwLock<HashMap<String, String>>>,
}

pub struct PersistentKeyValueStore {
    config: Config,
    data: Vec<Bucket>, // TODO: cache
    wal: Arc<Mutex<snapshots::SnapshotWriter>>,
    snapshot_set: Arc<Mutex<Box<dyn snapshot_set::SnapshotSet>>>,
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
    pub fn new(path: &Path, config: Config) -> Result<Self, Box<dyn Error>> {
        Self::new_with_snapshot_set(Box::new(snapshot_set::FileSnapshotSet::new(path)?), config)
    }

    pub fn new_with_snapshot_set(
        mut snapshot_set: Box<dyn SnapshotSet>,
        config: Config
    ) -> Result<Self, Box<dyn Error>> {
        let mut data = Vec::with_capacity(config.memory_bucket_count);
        for _ in 0..config.memory_bucket_count {
            data.push(Bucket {
                data: RwLock::new(HashMap::new()).into(),
            });
        }

        // Construct instance, imbuing it with a write-ahead log to persist new entries.
        let wal_path = &snapshot_set.create_or_get_snapshot(SnapshotType::Diff, true)?;
        let mut self_ = Self {
            config,
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
                            for (key, value) in snapshot_task.data_copy.into_iter() {
                                println!("snapshot ke {}", key);
                                writer
                                    .append_entry(Self::make_snapshot_entry(key, Some(&value)))
                                    .expect("Failed to write full snapshot entry to disk");
                            }
                            // TODO: verify that the snapshot was written correctly before publishing.
                            snapshot_set
                                .lock()
                                .unwrap()
                                .publish_completed_snapshot(snapshot_task.snapshot.ordinal, true)
                                .unwrap();
                            println!(
                                "PersistentKeyValueStore: published snapshot with ID {}",
                                snapshot_task.snapshot.ordinal
                            );
                        }
                    }
                }
            })
        );

        Ok(self_)
    }

    pub fn set(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>
    ) -> &mut PersistentKeyValueStore {
        let key = key.into();
        let value = value.into();

        // Hold lock on WAL throughout entire write operation to ensure that the
        // write-ahead log is consistently ordered.
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_entry(Self::make_snapshot_entry(key.clone(), Some(&value))).expect(
                "Failed to write set op to write-ahead log"
            );

            // Hold shorter lock on in-memory bucket to avoid blocking concurrent
            // readers on the full I/O operation.
            {
                let bucket = self.get_bucket(&key);
                let mut data = bucket.data.write().unwrap();
                data.insert(key, value);
            }

            self.maybe_trigger_full_snapshot(&mut wal);
        }
        self
    }

    pub fn unset(&mut self, key: impl Into<String>) {
        let key = key.into();
        let bucket = self.get_bucket(&key);

        // See notes on lock usage in set()
        let mut wal = self.wal.lock().unwrap();
        wal.append_entry(Self::make_snapshot_entry(key.clone(), None)).expect(
            "Failed to write unset op to write-ahead log"
        );
        {
            let mut data = bucket.data.write().unwrap();
            data.remove(&key);
        }

        self.maybe_trigger_full_snapshot(&mut wal);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let bucket = self.get_bucket(key);
        bucket.data.read().unwrap().get(key).cloned()
    }

    fn get_bucket(&self, key: &str) -> &Bucket {
        let hash = PersistentKeyValueStore::hash(key);
        let index = hash % (self.config.memory_bucket_count as u64);
        &self.data[index as usize]
    }

    fn hash(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn maybe_trigger_full_snapshot(&self, wal: &mut MutexGuard<snapshots::SnapshotWriter>) {
        // If reaching update threshold, trigger full snapshot write.
        // - Allocate the snapshot in a pending state.
        // - Instantly switch to a new WAL file with ordinal higher than the pending snapshot
        //   so concurrent writes can continue and will later be applied against this snapshot
        // - Signal the offline thread to carry out the snapshot, making a copy of the data.
        if
            (self.update_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1) %
                self.config.snapshot_interval == 0
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

    fn make_snapshot_entry(key: String, value: Option<&str>) -> snapshots::SnapshotEntry {
        snapshots::SnapshotEntry {
            key: key,
            value: value.map(|v| v.as_bytes().to_vec()).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path;

    use super::*;
    use tempfile::TempDir;

    fn file_length_in_bytes(path: &Path) -> u64 {
        File::open(path).unwrap().metadata().unwrap().len()
    }

    #[test]
    fn setget() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set("foo", "1");
            store.set("bar", "2");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("2")));
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
            assert_eq!(store2.get("foo"), Some(String::from("1")));
            assert_eq!(store2.get("bar"), Some(String::from("2")));
        }
    }

    #[test]
    fn get_nonexisting() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set("foo", "1");
            assert_eq!(store.get("bar"), None);
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
            assert_eq!(store2.get("bar"), None);
        }
    }

    #[test]
    fn set_overwrite() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set("foo", "1");
            store.set("bar", "2");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("2")));
            store.set("bar", "3");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            assert_eq!(store.get("bar"), Some(String::from("3")));
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
            assert_eq!(store2.get("foo"), Some(String::from("1")));
            assert_eq!(store2.get("bar"), Some(String::from("3")));
        }
    }

    #[test]
    fn unset() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set("foo", "1");
            assert_eq!(store.get("foo"), Some(String::from("1")));
            store.unset("foo");
            assert_eq!(store.get("foo"), None);
        }
        {
            let store2 = PersistentKeyValueStore::new(tmp_dir.path(), Config::default()).unwrap();
            assert_eq!(store2.get("foo"), None);
        }
    }

    #[test]
    fn creates_snapshot_has_expected_filesnapshotset() {
        let tmp_dir = TempDir::new().unwrap();

        // Create a store and set one key to trigger a snapshot.
        let mut config = Config::default();
        config.snapshot_interval = 1;
        PersistentKeyValueStore::new(tmp_dir.path(), config.clone()).unwrap().set("foo", "1");

        // Verify existence of snapshot files directly:
        //  (Ord=1, Diff) used to be the write-ahead log and has been deleted.
        //  (Ord=2, Full) is the current up to date snapshot.
        //  (Ord=3, Diff) is the new write-ahead log, which is empty.
        let snapshot_set = snapshot_set::FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 2);
        assert_eq!(snapshot_set.snapshots[0].ordinal, 2);
        assert_eq!(snapshot_set.snapshots[0].snapshot_type, SnapshotType::FullCompleted);
        assert_eq!(snapshot_set.snapshots[1].ordinal, 3);
        assert_eq!(snapshot_set.snapshots[1].snapshot_type, SnapshotType::Diff);
        assert!(
            File::open(&snapshot_set.snapshots[1].path).unwrap().metadata().unwrap().len() == 0
        );

        // Create a fresh store instance and make a modification to two more keys (triggers
        // snapshot) and then delete a third (does not trigger snapshot)
        let mut config = Config::default();
        config.snapshot_interval = 2;
        PersistentKeyValueStore::new(tmp_dir.path(), config.clone())
            .unwrap()
            .set("bar", "2")
            .set("baz", "3")
            .unset("foo");

        // Verify existence of snapshot files directly again.
        //  (Ord=2, Full) is the previous snapshot containing key `foo`
        //  (Ord=4, Full) is the new snapshot containing keys `foo` and `bar` (must be bigger than Ord=2)
        //  (Ord=5, Diff) is the new write-ahead log which contains the deletion of `foo` (non-empty)
        let snapshot_set = snapshot_set::FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 3);
        assert_eq!(snapshot_set.snapshots[0].ordinal, 2);
        assert_eq!(snapshot_set.snapshots[0].snapshot_type, SnapshotType::FullCompleted);
        assert_eq!(snapshot_set.snapshots[1].ordinal, 4);
        assert_eq!(snapshot_set.snapshots[1].snapshot_type, SnapshotType::FullCompleted);
        assert_eq!(snapshot_set.snapshots[2].ordinal, 5);
        assert_eq!(snapshot_set.snapshots[2].snapshot_type, SnapshotType::Diff);
        assert!(
            file_length_in_bytes(&snapshot_set.snapshots[0].path) <
            file_length_in_bytes(&snapshot_set.snapshots[1].path)
        );
        assert!(file_length_in_bytes(&snapshot_set.snapshots[2].path) > 0);
    }
}
