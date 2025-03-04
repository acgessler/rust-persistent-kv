use std::{
    borrow::Borrow,
    collections::HashMap,
    error::Error,
    hash::{ DefaultHasher, Hash, Hasher },
    ops::Range,
    sync::{ atomic::AtomicU64, Arc, Mutex, MutexGuard, RwLock },
};

use crate::config::{ Config, SyncMode };
use crate::snapshot_set::{ SnapshotInfo, SnapshotSet, SnapshotType, FileSnapshotSet };
use crate::snapshots::{ SnapshotReader, SnapshotWriter };

// Support switching between fixed (up to 8 bytes) and variable length keys.
// The former is used for all integer types and avoids allocations on the write path.
// On the read path, we always avoid allocations by borrowing as &u8 and leveraging
// HashMap's native support for looking up borrowed keys.
#[derive(Clone, PartialEq, Hash, Eq, Default)]
pub struct FixedLengthKey64Bit(pub [u8; 8]);

#[derive(Clone, PartialEq, Hash, Eq, Default)]
pub struct VariableLengthKey(pub Vec<u8>);

pub trait KeyAdapter: Clone + PartialEq + Hash + Eq + Borrow<[u8]> + ToOwned + From<Vec<u8>> {}

impl Borrow<[u8]> for FixedLengthKey64Bit {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl Borrow<[u8]> for VariableLengthKey {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for FixedLengthKey64Bit {
    fn from(v: Vec<u8>) -> Self {
        let mut key = [0; 8];
        key.copy_from_slice(&v);
        FixedLengthKey64Bit(key)
    }
}

impl From<Vec<u8>> for VariableLengthKey {
    fn from(v: Vec<u8>) -> Self {
        VariableLengthKey(v)
    }
}

impl KeyAdapter for FixedLengthKey64Bit {}
impl KeyAdapter for VariableLengthKey {}

// Hardcoded underlying implementations of store to keep generic interface small.
pub enum StoreImpl {
    FixedKey(Store<FixedLengthKey64Bit, FileSnapshotSet>),
    VariableKey(Store<VariableLengthKey, FileSnapshotSet>),
}

struct Bucket<TKey: KeyAdapter> {
    data: Arc<RwLock<HashMap<TKey, Vec<u8>>>>,
}

pub struct Store<TKey: KeyAdapter, TSnapshotSet: SnapshotSet> {
    config: Config,
    buckets: Vec<Bucket<TKey>>, // TODO: cache
    wal: Arc<Mutex<SnapshotWriter>>,
    snapshot_set: Arc<Mutex<TSnapshotSet>>,
    update_counter: AtomicU64,
    full_snapshot_writer_thread: Option<std::thread::JoinHandle<()>>,
    full_snapshot_writer_sender: Option<std::sync::mpsc::Sender<SnapshotTask>>,
}

struct SnapshotTask {
    raw_data: Vec<u8>,
    ranges: Vec<(Range<usize>, Range<usize>)>,
    snapshot: SnapshotInfo,
}

impl<TKey: KeyAdapter, TSS: SnapshotSet> Drop for Store<TKey, TSS> {
    fn drop(&mut self) {
        self.full_snapshot_writer_sender = None;
        self.full_snapshot_writer_thread.take().unwrap().join().unwrap();
    }
}

impl<TKey: KeyAdapter, TSS: SnapshotSet + 'static> Store<TKey, TSS> {
    pub fn new(mut snapshot_set: TSS, config: Config) -> Result<Self, Box<dyn Error>> {
        let mut data = Vec::with_capacity(config.memory_bucket_count);
        for _ in 0..config.memory_bucket_count {
            data.push(Bucket {
                data: RwLock::new(HashMap::new()).into(),
            });
        }

        // Construct instance, imbuing it with a write-ahead log to persist new entries.
        let wal_path = &snapshot_set.create_or_get_snapshot(SnapshotType::Diff, true)?;
        let snapshot_writer = SnapshotWriter::new(&wal_path.path, true, config.sync_mode);
        let mut self_ = Self {
            config,
            buckets: data,
            snapshot_set: Arc::new(Mutex::new(snapshot_set)),
            wal: Mutex::new(snapshot_writer).into(),
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
                            Self::write_and_finalize_snapshot_(&snapshot_set, snapshot_task);
                        }
                    }
                }
            })
        );

        Ok(self_)
    }

    /// Returns (number of entries, size in bytes)
    pub fn compute_size_info(&self) -> (usize, usize) {
        self.buckets
            .iter()
            .map(|e| {
                let bucket = e.data.read().unwrap();
                (
                    bucket.len(),
                    bucket
                        .iter()
                        .map(|(k, v)| k.borrow().len() + v.len())
                        .sum(),
                )
            })
            .reduce(|(ak, av), (ek, ev)| (ak + ek, av + ev))
            .unwrap()
    }

    pub fn set(&self, key: TKey, value: Vec<u8>) -> &Store<TKey, TSS> {
        let bucket = self.get_bucket_(key.borrow());
        // Hold lock on WAL throughout entire write operation to ensure that the
        // write-ahead log is consistent w.r.t. the in-memory map.
        // _append_op allows postponing writes/commit actions that do not need the lock.
        let _append_op = {
            let mut wal = self.wal.lock().unwrap();
            let append_op = wal
                .append_entry(key.borrow(), Some(&value))
                .expect("Failed to write set op to write-ahead log");

            // Hold shorter lock on in-memory bucket to avoid blocking concurrent
            // readers on the full I/O operation.
            {
                let mut data = bucket.data.write().unwrap();
                data.insert(key.clone(), value);
            }
            self.maybe_trigger_full_snapshot_(&mut wal);
            append_op
        };
        self
    }

    pub fn unset(&self, key: &[u8]) -> &Store<TKey, TSS> {
        let bucket = self.get_bucket_(key);

        // See notes on lock usage in set()
        let _append_op = {
            let mut wal = self.wal.lock().unwrap();
            let append_op = wal
                .append_entry(key, None)
                .expect("Failed to write unset op to write-ahead log");

            // Hold shorter lock on in-memory bucket to avoid blocking concurrent
            // readers on the full I/O operation.
            {
                let mut data = bucket.data.write().unwrap();
                data.remove(key);
            }
            self.maybe_trigger_full_snapshot_(&mut wal);
            append_op
        };
        self
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // TODO(acgessler) avoid Vec copy
        let bucket = self.get_bucket_(key);
        bucket.data.read().unwrap().get(key).cloned()
    }

    fn get_bucket_(&self, key: &[u8]) -> &Bucket<TKey> {
        let hash = Self::hash_(key);
        let index = hash % (self.config.memory_bucket_count as u64);
        &self.buckets[index as usize]
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
                    let key: TKey = entry.key.to_owned().into();
                    let bucket = self.get_bucket_(key.borrow());
                    let mut data = bucket.data.write().unwrap();

                    if entry.value.is_empty() {
                        data.remove(key.borrow());
                    } else {
                        data.insert(key, entry.value.clone());
                    }
                })
                .unwrap_or_else(|e|
                    panic!(
                        "Failed to read write-ahead log snapshot {:?} with error: {:?}",
                        snapshot_path,
                        e
                    )
                );
        }
        Ok(())
    }

    fn maybe_trigger_full_snapshot_(&self, wal: &mut MutexGuard<SnapshotWriter>) {
        // If reaching update threshold, trigger full snapshot write.
        // 1) Allocate the snapshot in a pending state and make a copy of all data now.
        // 2) Instantly switch to a new WAL file with ordinal higher than the pending snapshot
        //   so concurrent writes can continue and will later be applied against this snapshot
        // 3) Signal the offline thread to carry out the snapshot, sending the data copy.
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
                true,
                self.config.sync_mode
            );

            let mut raw_data = Vec::new();
            let mut ranges = Vec::new();
            for bucket in self.buckets.iter() {
                let data = bucket.data.read().unwrap();
                for (key, value) in data.iter() {
                    let key = key.borrow();
                    let start = raw_data.len();
                    raw_data.extend_from_slice(key);
                    raw_data.extend_from_slice(value);
                    ranges.push((
                        Range { start, end: start + key.len() },
                        Range { start: start + key.len(), end: raw_data.len() },
                    ));
                }
            }
            self.full_snapshot_writer_sender
                .as_ref()
                .unwrap()
                .send(SnapshotTask { raw_data, ranges, snapshot: pending_snapshot.clone() })
                .unwrap();
            // See write_and_finalize_snapshot_() below for the rest of the snapshot process.
        }
    }

    fn write_and_finalize_snapshot_(snapshot_set: &Mutex<TSS>, snapshot_task: SnapshotTask) {
        // No fsyncs required when writing individual snapshot entries. The implementation
        // of SnapshotWriter.drop includes a fsync covering all writes to the snapshot.
        let mut writer = SnapshotWriter::new(
            &snapshot_task.snapshot.path,
            false,
            SyncMode::NoExplicitSync
        );
        for (key_range, value_range) in snapshot_task.ranges.iter() {
            writer
                .append_entry(
                    &snapshot_task.raw_data[key_range.clone()],
                    Some(&snapshot_task.raw_data[value_range.clone()])
                )
                .expect("Failed to write full snapshot entry to disk");
        }
        drop(writer);
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
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use tempfile::TempDir;

    fn file_length_in_bytes(path: &Path) -> u64 {
        File::open(path).unwrap().metadata().unwrap().len()
    }

    fn create_store(tmp_dir: &TempDir) -> Store<VariableLengthKey, FileSnapshotSet> {
        Store::new(FileSnapshotSet::new(tmp_dir.path()).unwrap(), Config::default()).unwrap()
    }
    fn create_store_custom_config(
        tmp_dir: &TempDir,
        config: Config
    ) -> Store<VariableLengthKey, FileSnapshotSet> {
        Store::new(FileSnapshotSet::new(tmp_dir.path()).unwrap(), config).unwrap()
    }

    #[test]
    fn setget() {
        let tmp_dir = TempDir::new().unwrap();

        let store = create_store(&tmp_dir);
        store.set(VariableLengthKey(b"foo".to_vec()), b"1".to_vec());
        store.set(VariableLengthKey(b"bar".to_vec()), b"2".to_vec());
        assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"bar"), Some(b"2".to_vec()));
        drop(store);

        let store2 = create_store(&tmp_dir);
        assert_eq!(store2.get(b"foo"), Some(b"1".to_vec()));
        assert_eq!(store2.get(b"bar"), Some(b"2".to_vec()));
    }

    #[test]
    fn get_nonexisting() {
        let tmp_dir = TempDir::new().unwrap();
        let store = create_store(&tmp_dir);
        store.set(VariableLengthKey(b"foo".to_vec()), b"1".to_vec());
        assert_eq!(store.get(b"bar"), None);
        drop(store);

        let store2 = create_store(&tmp_dir);
        assert_eq!(store2.get(b"bar"), None);
    }

    #[test]
    fn set_overwrite() {
        let tmp_dir = TempDir::new().unwrap();

        let store = create_store(&tmp_dir);
        store.set(VariableLengthKey(b"foo".to_vec()), b"1".to_vec());
        store.set(VariableLengthKey(b"bar".to_vec()), b"2".to_vec());
        assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"bar"), Some(b"2".to_vec()));
        store.set(VariableLengthKey(b"bar".to_vec()), b"3".to_vec());
        assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"bar"), Some(b"3".to_vec()));
        drop(store);

        let store2 = create_store(&tmp_dir);
        assert_eq!(store2.get(b"foo"), Some(b"1".to_vec()));
        assert_eq!(store2.get(b"bar"), Some(b"3".to_vec()));
    }

    #[test]
    fn unset() {
        let tmp_dir = TempDir::new().unwrap();

        let store = create_store(&tmp_dir);
        store.set(VariableLengthKey(b"foo".to_vec()), b"1".to_vec());
        assert_eq!(store.get(b"foo"), Some(b"1".to_vec()));
        store.unset(b"foo");
        assert_eq!(store.get(b"foo"), None);
        drop(store);

        let store2 = create_store(&tmp_dir);
        assert_eq!(store2.get(b"foo"), None);
    }

    #[test]
    fn creates_snapshot_has_expected_filesnapshotset() {
        let tmp_dir = TempDir::new().unwrap();

        // Create a store and set one key to trigger a snapshot.
        create_store_custom_config(&tmp_dir, Config {
            snapshot_interval: 1,
            ..Config::default()
        }).set(VariableLengthKey(b"foo".to_vec()), b"1".to_vec());

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
        create_store_custom_config(&tmp_dir, Config {
            snapshot_interval: 2,
            ..Config::default()
        })
            .set(VariableLengthKey(b"bar".to_vec()), b"2".to_vec())
            .set(VariableLengthKey(b"baz".to_vec()), b"3".to_vec())
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
