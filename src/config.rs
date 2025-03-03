
#[derive(Clone, PartialEq, Debug)]
pub struct Config  {
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
    pub memory_bucket_count : usize,
}

impl Default for Config {

    fn default() -> Config {
        Self {
            snapshot_interval: 10000,
            memory_bucket_count: 32
        }
    }
}
