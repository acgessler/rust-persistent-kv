#[derive(Copy, Clone, PartialEq, Debug)]
pub enum SyncMode {
    /// Execute a full sync operation(s) after every single key write. This is
    /// _very_ slow (~milliseconds) but minimizes risk of data loss. If the local
    /// process fails, data loss is not possible. In the event of a OS level failure
    /// or power event, data loss is unlikely but still technically possible e.g.
    /// if the hardware further delays writes without the OS knowing.
    /// Note: The implementation uses File::sync_all(), so all caveats from there apply.
    BlockAndSync,

    /// Blocks on but does not explicitly sync file system operation(s). This is
    /// reasonably fast (~microseconds). In this mode, calling
    /// [`super::PersistentKeyValueStore::set`] or
    /// [`super::PersistentKeyValueStore::unset`] on a key still blocks on the
    /// syscall to append to the write log. This means that local process failures
    /// should not lead to data loss. OS level failures or power events are possible
    /// data loss concerning writes that occured in the seconds prior.
    BlockNoExplicitSync,

    /// Allows for in-memory buffering of writes before writing to disk. There
    /// is no limit on the size of frequency of the buffer, so at worst a disk
    /// write may not occur until the store instance is dropped.
    Buffered,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Config {
    /// If true, we will not write any snapshot / restore events to stdout.
    /// By default this is on as such events are rare and will be helpful to spot
    /// unexpected config or runtime issues.
    pub silent: bool,

    /// The interval at which full snapshots should be created. The unit counted is the
    /// number of key-value pairs modified (created, updated or deleted) meaning if
    /// `snapshot_interval` is 10,000, then a snapshot will be created every 10,000
    /// key-value pairs modified.
    ///
    /// Notes:
    ///  - Snapshots may be skipped if we cannot keep up with the number of changes.
    ///  - Snapshots are created asynchronously and do not block reads or writes to
    ///    the main store (except for a brief lock to create a memory data copy).
    ///  - Snapshots are not required for the store to have basic persistence
    ///    guarantees as all modifications are written to a write-ahead log first.
    ///    However, regular snapshotting compacts the write-ahead log and helps
    ///      keep disk usage and startup time to be bounded by
    ///         O(number of keys)
    ///      rather than
    ///         O(number of modifications)
    pub snapshot_interval: u64,

    /// The number of buckets to use for the memory store, each responsible for a part
    /// of the keyspace. This is a trade-off between memory overhead and contention
    /// avoidance in multithreaded operation. Each bucket has its own dictionary,
    /// supporting data structure and mutex.
    pub memory_bucket_count: usize,

    /// File system synchronization model.
    pub sync_mode: SyncMode,

    /// The number of threads to use for IO operations. This includes reading and writing
    /// of snapshots and influences (but not fully determines) number of shards used.
    pub target_io_parallelism_snapshots: u64,

    /// The number of shards to use for the write log (this is directly the file count used)
    pub target_io_parallelism_writelog: u64,

    /// The targeted size for a snapshot shard. This is not a hard limit. This number influences
    /// (but not fully determines) number of shards used for snapshots.
    pub target_snapshot_shard_size_bytes: usize,

    /// Whether to make use of positioned writes e.g. write_at() instead of seek() + write().
    /// This uses OS specific extensions and enables higher effective concurrency for writes.
    pub use_positioned_writes: bool,
}

impl Default for Config {
    fn default() -> Config {
        Self {
            snapshot_interval: 10000,
            memory_bucket_count: 32,
            sync_mode: SyncMode::BlockNoExplicitSync,
            silent: false,
            target_io_parallelism_snapshots: 8,
            target_io_parallelism_writelog: 1,
            target_snapshot_shard_size_bytes: 1024 * 1024 * 1024, // 1 GB

            #[cfg(target_os = "windows")]
            use_positioned_writes: false,
            #[cfg(not(target_os = "windows"))]
            use_positioned_writes: true,
        }
    }
}
