pub mod admin;
mod file_snapshot_set;

use std::{io, path::PathBuf};

/// Classifications of different snapshot files (used in conjunction with SnapshotSet)
#[derive(PartialEq, Clone, Debug, Copy)]
pub enum SnapshotType {
    /// A differential snapshot containing set and unset operations in sequence.
    /// Used for write-ahead-log snapshot files.
    Diff,
    /// A full snapshot containing all key-value pairs at a given point in time,
    /// assessed as complete and up-to-date i.e. the latest snapshot of this type.
    FullCompleted,
    /// A snapshot that did not complete successfully, e.g. due to a power failure
    /// or process panic. This snapshot is incomplete and should not be used.
    Pending,
}

/// Ordinal number type used for sequencing snapshots. Snapshots with higher ordinal numbers must
/// be applied after snapshots with lower ordinal numbers. Shards of the same snapshot can be
/// applied in parallel.
#[derive(PartialEq, PartialOrd, Eq, Ord, Hash, Clone, Debug, Copy)]
pub struct SnapshotOrdinal(pub u64);

impl std::fmt::Display for SnapshotOrdinal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Information about a snapshot. Always matches the state on disk, i.e. if a snapshot has
/// a SnapshotInfo instance the corresponding files also exist, though they may be empty.
#[derive(Clone, PartialEq, Debug)]
pub struct SnapshotInfo {
    pub snapshot_type: SnapshotType,
    pub ordinal: SnapshotOrdinal,
    // Paths to the different shards that make up the snapshot. Shards are guaranteed
    // to contain distinct keys and can thus be read in parallel.
    //
    // Shards are usually placed in numbered files containing the index and shard count.
    // However, once a snapshot is parsed, this information should no longer be used
    // and shard_paths is the canonical source of the ordering and number of shards.
    // This ensures we support recovery from broken state.
    pub shard_paths: Vec<PathBuf>,
}

impl SnapshotInfo {
    #[cfg(test)]
    pub fn single_shard_path(&self) -> PathBuf {
        assert!(
            self.shard_paths.len() == 1,
            "This snapshotInfo should have exactly one path"
        );
        self.shard_paths[0].clone()
    }
}

/// A set of snapshot files, used to manage and query snapshots in a given folder.
/// Provides operations to query snapshots that exist with their types and to add
/// new snapshot file (names) to the set.
///
/// This type only manages file names and creates/removes files as a whole, it does
/// not actually read/write snapshots _contents_.
///
/// Snapshot files are named in the format `snapshot_<ordinal>_<type>.bin` where
/// `<ordinal>` is a monotonically increasing number and `<type>` is one of
/// `diff`, `full`, or `pending` where `pending` should be renamed to `full`
/// once the snapshot is complete and published.
///
/// <ordinal> is used to determine the order of snapshots independent of file
/// system modification times which could be tampered with.
///
/// TODO(acgessler): Add file based lock to prevent accidental construction of
/// multiple SnapshotSet instances for the same folder.
pub trait SnapshotSet: Send {
    /// Registers a new snapshot path usable for the given snapshot type.
    /// This will return a new snapshot path that can be used to write the snapshot to.
    ///
    /// If `may_append_existing` is set to true, an existing snapshot file may be returned
    /// if there is no (completed) full snapshot with a higher ordinal.
    ///
    /// A new snapshot file will have a ordinal higher than all previous snapshots.
    /// A new shapshot file will be created with empty contents.
    fn create_or_get_snapshot(
        &mut self,
        snapshot_type: SnapshotType,
        shard_count: u64,
        may_append_existing: bool,
    ) -> Result<SnapshotInfo, io::Error>;

    /// Publishes a pending snapshot as a full snapshot. This will rename the snapshot file
    /// to indicate that it is a full snapshot and considered complete.
    /// `purge_obsolete_diff_snapshots` specifies if differential snapshots that
    /// are now obsolete should be auto-deleted.
    /// `purge_obsolete_pending_snapshots` specifies if pending snapshots that
    /// precede the now completed snapshot should be auto-deleted.
    fn publish_completed_snapshot(
        &mut self,
        pending_snapshot_ordinal: SnapshotOrdinal,
        purge_obsolete_diff_snapshots: bool,
        purge_obsolete_pending_snapshots: bool,
    ) -> Result<(), io::Error>;

    /// Returns all snapshots that need to be restored in order to get the latest state.
    /// This includes the latest full snapshot and all differential snapshots since then.
    fn get_snapshots_to_restore(&self) -> Vec<&SnapshotInfo>;
}

/// Primary file based implementation of the SnapshotSet trait.
pub use file_snapshot_set::FileSnapshotSet;