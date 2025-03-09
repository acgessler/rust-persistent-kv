use std::{
    borrow::Borrow,
    cmp::{ max, min },
    collections::HashMap,
    error::Error,
    hash::{ DefaultHasher, Hash, Hasher },
    io,
    ops::Range,
    path::Path,
    sync::{ atomic::AtomicU64, Arc, Mutex, RwLock },
    thread,
    time::Instant,
};

use crate::snapshot::{ SnapshotReader, SnapshotWriter, SnapshotWriterConfig };
use crate::snapshot_set::{ FileSnapshotSet, SnapshotSet, SnapshotType };
use crate::{ config::{ Config, SyncMode }, snapshot_set::SnapshotOrdinal };

// Support switching between fixed (up to 8 bytes) and variable length keys.
// The former is used for all integer types and avoids allocations on the write path.
// On the read path, we always avoid allocations by borrowing as &u8 and leveraging
// HashMap's native support for looking up borrowed keys.
#[derive(Clone, PartialEq, Hash, Eq, Default)]
pub struct FixedLengthKey64Bit(pub [u8; 8]);

#[derive(Clone, PartialEq, Hash, Eq, Default)]
pub struct VariableLengthKey(pub Vec<u8>);

pub trait KeyAdapter: Clone +
    PartialEq +
    Hash +
    Eq +
    Borrow<[u8]> +
    From<Vec<u8>> +
    Send +
    Sync +
    'static {}

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

#[derive(Clone)]
struct Bucket<TKey: KeyAdapter> {
    data: Arc<RwLock<HashMap<TKey, Vec<u8>>>>,
}

struct SnapshotTask;

pub struct Store<TKey: KeyAdapter, TSS: SnapshotSet + 'static> {
    config: Config,
    buckets: Vec<Bucket<TKey>>, // TODO: cache
    // Outer mutex held only to replace the list of writers. Inner mutex held to guard
    // write operations against a range of keys.
    wal: Arc<RwLock<Vec<Mutex<SnapshotWriter>>>>,
    snapshot_set: Arc<Mutex<TSS>>,
    update_counter: AtomicU64,
    full_snapshot_writer_thread: Option<std::thread::JoinHandle<()>>,
    full_snapshot_writer_sender: Option<std::sync::mpsc::Sender<SnapshotTask>>,
}

impl<TKey: KeyAdapter, TSS: SnapshotSet + 'static> Drop for Store<TKey, TSS> {
    fn drop(&mut self) {
        if self.full_snapshot_writer_thread.is_some() {
            self.stop_snapshot_writer_thread_();
        }
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
        let snapshot_writers = Self::create_write_ahead_log_(&mut snapshot_set, &config, true)?;
        let mut self_ = Self {
            config,
            buckets: data,
            snapshot_set: Arc::new(Mutex::new(snapshot_set)),
            wal: Arc::new(RwLock::new(snapshot_writers)),
            full_snapshot_writer_thread: None,
            full_snapshot_writer_sender: None,
            update_counter: AtomicU64::new(0),
        };

        // Restore state from past snapshots. This will read the last full snapshot,
        // if available, and then replay all write-ahead log entries since that snapshot.
        self_.restore_from_snapshots_()?;

        self_.start_snapshot_writer_thread_();
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
        let (bucket, hash) = Self::get_bucket_(&self.buckets, key.borrow());
        // Hold lock on WAL throughout entire write operation to ensure that the
        // write-ahead log is consistent w.r.t. the in-memory map.
        // _append_op allows postponing writes/commit actions that do not need the lock.
        (
            {
                let wal_outer = self.wal.read().unwrap();
                let mut wal = wal_outer[(hash % (wal_outer.len() as u64)) as usize].lock().unwrap();
                let append_op = wal
                    .sequence_entry(key.borrow(), Some(&value))
                    .expect("Failed to sequence set op to write-ahead log");

                // Hold shorter lock on in-memory bucket to avoid blocking concurrent
                // readers on the full I/O operation.
                {
                    let mut data = bucket.data.write().unwrap();
                    data.insert(key.clone(), value);
                }
                self.maybe_trigger_full_snapshot_();
                append_op
            }
        )
            .commit()
            .expect("Failed to commit set op to write-ahead log");
        self
    }

    pub fn unset(&self, key: &[u8]) -> &Store<TKey, TSS> {
        let (bucket, hash) = Self::get_bucket_(&self.buckets, key);

        // See notes on lock usage in set()
        (
            {
                let wal_outer = self.wal.read().unwrap();
                let mut wal = wal_outer[(hash % (wal_outer.len() as u64)) as usize].lock().unwrap();
                let append_op = wal
                    .sequence_entry(key, None)
                    .expect("Failed to sequence unset op to write-ahead log");

                // Hold shorter lock on in-memory bucket to avoid blocking concurrent
                // readers on the full I/O operation.
                {
                    let mut data = bucket.data.write().unwrap();
                    data.remove(key);
                }
                self.maybe_trigger_full_snapshot_();
                append_op
            }
        )
            .commit()
            .expect("Failed to commit unset op to write-ahead log");
        self
    }

    #[allow(dead_code)]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_convert(key, |v| v.map(|v| v.to_vec()))
    }

    /// Avoids copying the value to a new Vec and passed it to a FnOnce for processing
    /// instead. Internal locks are held for the duration of f() and f()'s implementation
    /// may not block or call back into the store.
    pub fn get_convert<F, V>(&self, key: &[u8], f: F) -> V where F: FnOnce(Option<&[u8]>) -> V {
        let (bucket, _hash) = Self::get_bucket_(&self.buckets, key);
        f(
            bucket.data
                .read()
                .unwrap()
                .get(key)
                .map(|v| v.as_slice())
        )
    }

    #[cfg(test)]
    pub fn testonly_wait_for_pending_snapshots(&mut self) -> &mut Store<TKey, TSS> {
        self.stop_snapshot_writer_thread_();
        self.start_snapshot_writer_thread_();
        self
    }

    fn get_bucket_<'t>(buckets: &'t [Bucket<TKey>], key: &[u8]) -> (&'t Bucket<TKey>, u64) {
        let hash = Self::hash_(key);
        let index = hash % (buckets.len() as u64);
        (&buckets[index as usize], hash)
    }

    fn hash_(key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new(); // TODO(acgessler) re-use?
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn create_write_ahead_log_(
        snapshot_set: &mut TSS,
        config: &Config,
        may_append_existing: bool
    ) -> Result<Vec<Mutex<SnapshotWriter>>, io::Error> {
        let wal_path = &snapshot_set.create_or_get_snapshot(
            SnapshotType::Diff,
            config.target_io_parallelism_writelog,
            may_append_existing
        )?;
        Ok(
            (0..wal_path.shard_paths.len())
                .map(|i| {
                    Mutex::new(
                        SnapshotWriter::new(&wal_path.shard_paths[i], true, SnapshotWriterConfig {
                            sync_mode: config.sync_mode,
                            use_positioned_writes: config.use_positioned_writes,
                        })
                    )
                })
                .collect()
        )
    }

    fn restore_from_snapshots_(&mut self) -> Result<(), Box<dyn Error>> {
        let start_time = Instant::now();
        let snapshot_set = self.snapshot_set.lock().unwrap();
        let snapshots_to_restore = snapshot_set.get_snapshots_to_restore();
        for snapshot_path in snapshots_to_restore.iter() {
            // We don't place assumptions which buckets are in which shards and that keys are
            // in their correct buckets, which allows us to change the hashing function or
            // config and still recover the snapshots. If there was no hash or bucket altering
            // change each shard maps to n consecutive buckets and the locks below will be
            // uncontested and thus near-free.
            let shard_paths = snapshot_path.shard_paths.clone();
            let buckets = &self.buckets;
            thread
                ::scope(
                    |s| -> Result<(), String> {
                        for chunk in shard_paths.chunks(
                            snapshot_path.shard_paths
                                .len()
                                .div_ceil(self.config.target_io_parallelism_snapshots as usize)
                        ) {
                            s.spawn(
                                move || -> Result<(), String> {
                                    for shard in chunk {
                                        SnapshotReader::new(shard)
                                            .read_entries(|entry| {
                                                let key: TKey = entry.key.to_owned().into();
                                                let bucket = Self::get_bucket_(
                                                    buckets,
                                                    key.borrow()
                                                ).0;
                                                let mut data = bucket.data.write().unwrap();

                                                if entry.value.is_empty() {
                                                    data.remove(key.borrow());
                                                } else {
                                                    data.insert(key, entry.value.clone());
                                                }
                                            })
                                            .map_err(|e|
                                                format!(
                                                    "Failed to read write-ahead log snapshot {:?} with error: {:?}",
                                                    snapshot_path,
                                                    e
                                                )
                                            )?;
                                    }
                                    Ok(())
                                }
                            );
                        }
                        Ok(())
                    }
                )
                .unwrap();
        }
        let elapsed = start_time.elapsed();
        if !self.config.silent {
            println!(
                "PersistentKeyValueStore: restored from snapshots with IDs {:?}; duration: {:?}",
                snapshots_to_restore
                    .iter()
                    .map(|e| e.ordinal)
                    .collect::<Vec<SnapshotOrdinal>>(),
                elapsed
            );
        }
        Ok(())
    }

    fn start_snapshot_writer_thread_(&mut self) {
        assert!(self.full_snapshot_writer_thread.is_none());
        // Start a thread that periodically writes full snapshots to disk.
        let (sender, receiver) = std::sync::mpsc::channel();
        let snapshot_set = self.snapshot_set.clone();
        let wal_ref = self.wal.clone();
        let buckets_ref = self.buckets.clone();
        let config_clone = self.config.clone();
        self.full_snapshot_writer_sender = Some(sender);
        self.full_snapshot_writer_thread = Some(
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
                            Self::write_and_finalize_snapshot_(
                                &config_clone,
                                &wal_ref,
                                &buckets_ref,
                                &snapshot_set,
                                snapshot_task
                            );
                        }
                    }
                }
            })
        );
    }

    fn stop_snapshot_writer_thread_(&mut self) {
        assert!(self.full_snapshot_writer_thread.is_some());
        self.full_snapshot_writer_sender = None;
        self.full_snapshot_writer_thread.take().unwrap().join().unwrap();
    }

    fn maybe_trigger_full_snapshot_(&self) {
        // If reaching update threshold, trigger full snapshot write.
        if
            (self.update_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1) %
                self.config.snapshot_interval == 0
        {
            self.full_snapshot_writer_sender.as_ref().unwrap().send(SnapshotTask).unwrap();
            // See write_and_finalize_snapshot_() below for the rest of the snapshot process.
        }
    }

    fn write_and_finalize_snapshot_(
        config: &Config,
        wal_ref: &RwLock<Vec<Mutex<SnapshotWriter>>>,
        buckets: &Vec<Bucket<TKey>>,
        snapshot_set: &Mutex<TSS>,
        _snapshot_task: SnapshotTask
    ) {
        let start_time = Instant::now();
        // Snapshot procedure:
        // 1) Allocate the snapshot in a pending state.
        // 2) Atomically switch to a new WAL file with ordinal higher than the pending snapshot
        //   so concurrent writes can continue and will later be applied against this snapshot
        // 3) Start copying the in-memory buckets one by one to a separate buffer (to keep
        //   overall lock time low) and then write them to the snapshot file.
        // 4) Publish the snapshots as completed.
        //
        // Note: this approach has 1/bucket_size memory overhead and minimizes lock time for
        // concurrent reads and writes. However, it does not guarantee that the snapshot is
        // exactly at the point in time when the snapshot was triggered and the individual
        // buckets are copied at different points in time. This is ok since any later change
        // included in the snapshot is also in the WAL so would be applied during restore
        // anyway. The downside is that snapshots aren't deterministic w.r.t the state of
        // the in-memory store at the time the snapshot threshold was reached.
        let mut snapshot_set = snapshot_set.lock().unwrap();

        let shard_count = Self::recommend_shard_count_(buckets, config);

        let pending_snapshot = {
            // Disallow concurrent writes to the store for this section only so
            // we can atomically switch to a new WAL file (set of files if sharded)
            // TODO: handle error propagation.
            let mut wal_outer = wal_ref.write().unwrap();
            let pending_snapshot = snapshot_set
                .create_or_get_snapshot(SnapshotType::Pending, shard_count as u64, false)
                .unwrap();
            *wal_outer = Self::create_write_ahead_log_(&mut snapshot_set, config, false).unwrap();
            pending_snapshot
        };

        let shards = (0..shard_count).collect::<Vec<usize>>();
        let buckets_per_shard = buckets.len().div_ceil(shard_count);
        thread
            ::scope(
                |s| -> Result<(), String> {
                    let pending_snapshot = &pending_snapshot;
                    let buckets = &buckets;

                    for chunk in shards.chunks(
                        shard_count.div_ceil(config.target_io_parallelism_snapshots as usize)
                    ) {
                        s.spawn(
                            move || -> Result<(), String> {
                                for shard in chunk {
                                    let bucket_range = shard * buckets_per_shard..min(
                                        buckets.len(),
                                        (shard + 1) * buckets_per_shard
                                    );
                                    Self::write_buckets_to_snapshot_(
                                        &pending_snapshot.shard_paths[*shard],
                                        &buckets[bucket_range]
                                    ).map_err(|e| e.to_string())?;
                                }
                                Ok(())
                            }
                        );
                    }
                    Ok(())
                }
            )
            .unwrap();

        // TODO(acgessler): verify that the snapshot was written correctly before publishing.
        snapshot_set.publish_completed_snapshot(pending_snapshot.ordinal, true, true).unwrap();

        if !config.silent {
            let elapsed = start_time.elapsed();
            println!(
                "PersistentKeyValueStore: published snapshot with ID {}; duration: {:?}",
                pending_snapshot.ordinal,
                elapsed
            );
        }
    }

    fn write_buckets_to_snapshot_(
        path: &Path,
        buckets: &[Bucket<TKey>]
    ) -> Result<(), Box<dyn Error>> {
        // No fsyncs required when writing individual snapshot entries. The implementation
        // of SnapshotWriter.drop includes a fsync covering all writes to the snapshot.
        let mut writer = SnapshotWriter::new(path, false, SnapshotWriterConfig {
            sync_mode: SyncMode::NoExplicitSync,
            use_positioned_writes: false,
        });

        let mut raw_data = Vec::new();
        let mut ranges = Vec::new();
        for bucket in buckets.iter() {
            raw_data.clear();
            ranges.clear();
            Self::fast_copy_bucket_to_ranges_(
                &bucket.data.read().unwrap(), // Hold bucket read lock
                &mut raw_data,
                &mut ranges
            );

            for (key_range, value_range) in ranges.iter() {
                writer
                    .sequence_entry(
                        &raw_data[key_range.clone()],
                        Some(&raw_data[value_range.clone()])
                    )?
                    .commit()?;
            }
        }
        Ok(())
    }

    fn fast_copy_bucket_to_ranges_(
        bucket_data: &HashMap<TKey, Vec<u8>>,
        raw_data: &mut Vec<u8>,
        ranges: &mut Vec<(Range<usize>, Range<usize>)>
    ) {
        for (key, value) in bucket_data.iter() {
            let key = key.borrow();
            let start = raw_data.len();
            raw_data.extend_from_slice(key);
            raw_data.extend_from_slice(value);
            ranges.push((
                Range {
                    start,
                    end: start + key.len(),
                },
                Range {
                    start: start + key.len(),
                    end: raw_data.len(),
                },
            ));
        }
    }

    fn recommend_shard_count_(buckets: &[Bucket<TKey>], config: &Config) -> usize {
        // Estimate the number of shards required to keep the snapshot size below the
        // target size. This is a rough estimate of serialized size.
        let size_bytes: usize = buckets
            .iter()
            .map(|e| {
                e.data
                    .read()
                    .unwrap()
                    .iter()
                    .map(
                        |(k, v)|
                            k.borrow().len() +
                            v.len() +
                            6 /* proto overhead: 2 tags, encoded varint length */
                    )
                    .sum::<usize>()
            })
            .sum();
        let target_size = config.target_snapshot_shard_size_bytes;
        let shard_count = size_bytes.div_ceil(target_size);
        const MIN_SIZE_FOR_PARALLEL_IO: usize = 100 * 1024; // 100 KiB
        if shard_count == 1 && size_bytes < MIN_SIZE_FOR_PARALLEL_IO {
            1
        } else {
            max(shard_count, config.target_io_parallelism_snapshots as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ fs::File, path::Path };

    use crate::snapshot_set::SnapshotOrdinal;

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
        println!("{:?}", snapshot_set);
        assert_eq!(snapshot_set.snapshots.len(), 2);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(2));
        assert_eq!(snapshot_set.snapshots[0].snapshot_type, SnapshotType::FullCompleted);
        assert_eq!(snapshot_set.snapshots[1].ordinal, SnapshotOrdinal(3));
        assert_eq!(snapshot_set.snapshots[1].snapshot_type, SnapshotType::Diff);
        assert!(
            File::open(&snapshot_set.snapshots[1].shard_paths[0])
                .unwrap()
                .metadata()
                .unwrap()
                .len() == 0
        );

        // Create a fresh store instance and make a modification to two more keys (triggers
        // snapshot) and then delete a third (does not trigger snapshot)
        let mut store = create_store_custom_config(&tmp_dir, Config {
            snapshot_interval: 2,
            ..Config::default()
        });
        store.set(VariableLengthKey(b"bar".to_vec()), b"2".to_vec());
        store.set(VariableLengthKey(b"baz".to_vec()), b"3".to_vec());
        store.testonly_wait_for_pending_snapshots();
        store.unset(b"foo");
        drop(store);

        // Verify existence of snapshot files directly again.
        //  (Ord=2, Full) is the previous snapshot containing key `foo`
        //  (Ord=4, Full) is the new snapshot containing keys `foo` and `bar` (must be bigger than Ord=2)
        //  (Ord=5, Diff) is the new write-ahead log which contains the deletion of `foo` (non-empty)
        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 3);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(2));
        assert_eq!(snapshot_set.snapshots[0].snapshot_type, SnapshotType::FullCompleted);
        assert_eq!(snapshot_set.snapshots[1].ordinal, SnapshotOrdinal(4));
        assert_eq!(snapshot_set.snapshots[1].snapshot_type, SnapshotType::FullCompleted);
        assert_eq!(snapshot_set.snapshots[2].ordinal, SnapshotOrdinal(5));
        assert_eq!(snapshot_set.snapshots[2].snapshot_type, SnapshotType::Diff);
        assert!(
            file_length_in_bytes(&snapshot_set.snapshots[0].shard_paths[0]) <
                file_length_in_bytes(&snapshot_set.snapshots[1].shard_paths[0])
        );
        assert!(file_length_in_bytes(&snapshot_set.snapshots[2].shard_paths[0]) > 0);
    }
}
