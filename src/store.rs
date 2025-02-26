use std::{
    collections::HashMap,
    error::Error,
    hash::{ DefaultHasher, Hash, Hasher },
    path::Path,
    sync::{ atomic::AtomicU64, Arc, Mutex, MutexGuard, RwLock },
};

use crate::config::Config;
use crate::snapshot_set::{ SnapshotInfo, SnapshotSet, SnapshotType, FileSnapshotSet };
use crate::snapshots::{ SnapshotReader, SnapshotWriter, SnapshotEntry };

struct Bucket {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

pub struct PersistentRawKeyValueStore {
    config: Config,
    data: Vec<Bucket>, // TODO: cache
    wal: Arc<Mutex<SnapshotWriter>>,
    snapshot_set: Arc<Mutex<Box<dyn SnapshotSet>>>,
    update_counter: AtomicU64,
    full_snapshot_writer_thread: Option<std::thread::JoinHandle<()>>,
    full_snapshot_writer_sender: Option<std::sync::mpsc::Sender<SnapshotTask>>,
}

struct SnapshotTask {
    data_copy: HashMap<Vec<u8>, Vec<u8>>,
    snapshot: SnapshotInfo,
}

impl Drop for PersistentRawKeyValueStore {
    fn drop(&mut self) {
        self.full_snapshot_writer_sender = None;
        self.full_snapshot_writer_thread.take().unwrap().join().unwrap();
    }
}

impl PersistentRawKeyValueStore {
    pub fn new(path: &Path, config: Config) -> Result<Self, Box<dyn Error>> {
        Self::new_with_snapshot_set(Box::new(FileSnapshotSet::new(path)?), config)
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
            wal: Mutex::new(SnapshotWriter::new(&wal_path.path, true)).into(),
            full_snapshot_writer_thread: None,
            full_snapshot_writer_sender: None,
            update_counter: AtomicU64::new(0),
        };

        // Restore state from past snapshots. This will read the last full snapshot,
        // if available, and then replay all write-ahead log entries since that snapshot.
        self_.restore_from_snapshots_()?;

        // Start a thread that periodically writes full snapshots to disk.
        let (sender, receiver) = std::sync::mpsc::channel();
        let snapshot_set = self_.snapshot_set.clone();
        self_.full_snapshot_writer_sender = Some(sender);
        self_.full_snapshot_writer_thread = Some(
            std::thread::spawn(move || {
                loop {
                    match receiver.recv() {
                        Err(_) => {
                            break;
                        }
                        Ok(mut snapshot_task) => {
                            // Skip over any queued snapshots that we're not keeping up with and process the latest only.
                            while let Ok(queued_snapshot_task) = receiver.try_recv() {
                                snapshot_task = queued_snapshot_task;
                            }
                            Self::write_and_finalize_snapshot_(&*snapshot_set, snapshot_task);
                        }
                    }
                }
            })
        );

        Ok(self_)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> &mut PersistentRawKeyValueStore {
        // Hold lock on WAL throughout entire write operation to ensure that the
        // write-ahead log is consistently ordered.
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_entry(Self::make_snapshot_entry(key, Some(&value))).expect(
                "Failed to write set op to write-ahead log"
            );

            // Hold shorter lock on in-memory bucket to avoid blocking concurrent
            // readers on the full I/O operation.
            {
                let bucket = self.get_bucket_(&key);
                let mut data = bucket.data.write().unwrap();
                data.insert(key.to_vec(), value.to_vec());
            }

            self.maybe_trigger_full_snapshot_(&mut wal);
        }
        self
    }

    pub fn unset(&mut self, key: &[u8]) -> &mut PersistentRawKeyValueStore {
        let bucket = self.get_bucket_(&key);

        // See notes on lock usage in set()
        {
            let mut wal = self.wal.lock().unwrap();
            wal.append_entry(Self::make_snapshot_entry(key, None)).expect(
                "Failed to write unset op to write-ahead log"
            );
            {
                let mut data = bucket.data.write().unwrap();
                data.remove(key);
            }

            self.maybe_trigger_full_snapshot_(&mut wal);
        }
        self
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // TODO(acgessler) avoid Vec copy
        let bucket = self.get_bucket_(key);
        bucket.data.read().unwrap().get(key).cloned()
    }

    fn get_bucket_(&self, key: &[u8]) -> &Bucket {
        let hash = PersistentRawKeyValueStore::hash_(key);
        let index = hash % (self.config.memory_bucket_count as u64);
        &self.data[index as usize]
    }

    fn hash_(key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new(); // TODO(acgessler) re-use?
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn restore_from_snapshots_(&mut self) -> Result<(), Box<dyn Error>> {
        let snapshot_set = self.snapshot_set.lock().unwrap();
        let snapshots_to_restore = snapshot_set.get_snapshots_to_restore();
        for snapshot_path in snapshots_to_restore {
            SnapshotReader::new(&snapshot_path.path)
                .read_entries(|entry| {
                    let bucket = self.get_bucket_(&entry.key);
                    let mut data = bucket.data.write().unwrap();

                    if entry.value.is_empty() {
                        data.remove(&entry.key);
                    } else {
                        data.insert(entry.key, entry.value);
                    }
                })
                .expect(
                    format!("Failed to read write-ahead log snapshot: {:?}", snapshot_path).as_str()
                );
        }
        Ok(())
    }

    fn maybe_trigger_full_snapshot_(&self, wal: &mut MutexGuard<SnapshotWriter>) {
        // If reaching update threshold, trigger full snapshot write.
        // 1) Allocate the snapshot in a pending state and make a copy of all data now.
        // 2) Instantly switch to a new WAL file with ordinal higher than the pending snapshot
        //   so concurrent writes can continue and will later be applied against this snapshot
        // 3) Signal the offline thread to carry out the snapshot.
        if
            (self.update_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1) %
                self.config.snapshot_interval == 0
        {
            let mut snapshot_set = self.snapshot_set.lock().unwrap();
            let pending_snapshot = snapshot_set
                .create_or_get_snapshot(SnapshotType::Pending, false)
                .unwrap();
            **wal = SnapshotWriter::new(
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
            // See write_and_finalize_snapshot_() below for the rest of the snapshot process.
        }
    }

    fn write_and_finalize_snapshot_(
        snapshot_set: &Mutex<Box<dyn SnapshotSet>>,
        snapshot_task: SnapshotTask
    ) {
        let mut writer = SnapshotWriter::new(&snapshot_task.snapshot.path, false);
        for (key, value) in snapshot_task.data_copy.into_iter() {
            writer
                .append_entry(Self::make_snapshot_entry(&key, Some(&value)))
                .expect("Failed to write full snapshot entry to disk");
        }
        // TODO(acgessler): verify that the snapshot was written correctly before publishing.
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

    fn make_snapshot_entry(key: &[u8], value: Option<&[u8]>) -> SnapshotEntry {
        SnapshotEntry {
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use tempfile::TempDir;

    fn file_length_in_bytes(path: &Path) -> u64 {
        File::open(path).unwrap().metadata().unwrap().len()
    }

    #[test]
    fn setget() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set(b"foo", b"1");
            store.set(b"bar", b"2");
            assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
            assert_eq!(store.get(b"bar"), Some(b"2".to_vec()));
        }
        {
            let store2 = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            assert_eq!(store2.get(b"foo"), Some(b"1".to_vec()));
            assert_eq!(store2.get(b"bar"), Some(b"2".to_vec()));
        }
    }

    #[test]
    fn get_nonexisting() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set(b"foo", b"1");
            assert_eq!(store.get(b"bar"), None);
        }
        {
            let store2 = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            assert_eq!(store2.get(b"bar"), None);
        }
    }

    #[test]
    fn set_overwrite() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set(b"foo", b"1");
            store.set(b"bar", b"2");
            assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
            assert_eq!(store.get(b"bar"), Some(b"2".to_vec()));
            store.set(b"bar", b"3");
            assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
            assert_eq!(store.get(b"bar"), Some(b"3".to_vec()));
        }
        {
            let store2 = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            assert_eq!(store2.get(b"foo"), Some(b"1".to_vec()));
            assert_eq!(store2.get(b"bar"), Some(b"3".to_vec()));
        }
    }

    #[test]
    fn unset() {
        let tmp_dir = TempDir::new().unwrap();
        {
            let mut store = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            store.set(b"foo", b"1");
            assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
            store.unset(b"foo");
            assert_eq!(store.get(b"foo"), None);
        }
        {
            let store2 = PersistentRawKeyValueStore::new(
                tmp_dir.path(),
                Config::default()
            ).unwrap();
            assert_eq!(store2.get(b"foo"), None);
        }
    }

    #[test]
    fn creates_snapshot_has_expected_filesnapshotset() {
        let tmp_dir = TempDir::new().unwrap();

        // Create a store and set one key to trigger a snapshot.
        let mut config = Config::default();
        config.snapshot_interval = 1;
        PersistentRawKeyValueStore::new(tmp_dir.path(), config.clone()).unwrap().set(b"foo", b"1");

        // Verify existence of snapshot files directly:
        //  (Ord=1, Diff) used to be the write-ahead log and has been deleted.
        //  (Ord=2, Full) is the current up to date snapshot.
        //  (Ord=3, Diff) is the new write-ahead log, which is empty.
        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
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
        PersistentRawKeyValueStore::new(tmp_dir.path(), config.clone())
            .unwrap()
            .set(b"bar", b"2")
            .set(b"baz", b"3")
            .unset(b"foo");

        // Verify existence of snapshot files directly again.
        //  (Ord=2, Full) is the previous snapshot containing key `foo`
        //  (Ord=4, Full) is the new snapshot containing keys `foo` and `bar` (must be bigger than Ord=2)
        //  (Ord=5, Diff) is the new write-ahead log which contains the deletion of `foo` (non-empty)
        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
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
