use std::fs;

use super::{FileSnapshotSet, SnapshotType};

/// Admin operations for a snapshot set, e.g. pruning snapshots.
#[allow(dead_code)]
pub trait SnapshotSetAdmin: Send {
    /// Prunes backup snapshots, keeping only the latest `max_backups_keep` full snapshots.
    /// This is useful to limit the number of backup snapshots that are kept around.
    /// __Warning__: This will delete files from the file system.
    fn prune_backup_snapshots(&mut self, max_backups_keep: usize) -> Result<(), std::io::Error>;

    /// Prunes snapshots that are not completed, e.g. due to a power failure or process panic.
    /// This is useful to clean up incomplete snapshots that should not be used.
    /// __Warning__: This will delete files from the file system.
    fn prune_not_completed_snapshots(&mut self) -> Result<(), std::io::Error>;
}

impl SnapshotSetAdmin for FileSnapshotSet {
    fn prune_backup_snapshots(&mut self, max_backups_keep: usize) -> Result<(), std::io::Error> {
        let mut full_backup_snapshots = self
            .snapshots
            .iter()
            .filter(|snapshot| snapshot.snapshot_type == SnapshotType::FullCompleted)
            .cloned()
            .collect::<Vec<_>>();

        full_backup_snapshots.sort_by_key(|snapshot| snapshot.ordinal);
        // Skip last item, this is the latest snapshot which we never consider a backup.
        let full_backup_snapshots = &full_backup_snapshots[..full_backup_snapshots.len() - 1];
        if max_backups_keep >= full_backup_snapshots.len() {
            return Ok(());
        }

        let num_snapshots_to_delete = full_backup_snapshots.len() - max_backups_keep;
        for snapshot in full_backup_snapshots.iter().take(num_snapshots_to_delete) {
            println!("Pruning backup snapshot: {:?}", snapshot.shard_paths);
            for path in snapshot.shard_paths.iter() {
                fs::remove_file(path)?
            }
            self.snapshots.retain(|s| s.ordinal != snapshot.ordinal);
        }
        Ok(())
    }

    fn prune_not_completed_snapshots(&mut self) -> Result<(), std::io::Error> {
        let not_completed_snapshots = self
            .snapshots
            .iter()
            .filter(|snapshot| snapshot.snapshot_type == SnapshotType::Pending)
            .cloned()
            .collect::<Vec<_>>();

        for snapshot in not_completed_snapshots.iter() {
            println!(
                "Pruning not completed snapshots: {:?}",
                snapshot.shard_paths
            );
            for path in snapshot.shard_paths.iter() {
                fs::remove_file(path)?
            }
            self.snapshots.retain(|s| s.ordinal != snapshot.ordinal);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        path::{Path, PathBuf},
    };
    use tempfile::TempDir;

    use super::*;
    use crate::snapshot_set::SnapshotOrdinal;

    fn create_temp_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn create_snapshot_file(folder: &Path, name: &str) -> PathBuf {
        let path = folder.join(name);
        File::create(&path).unwrap();
        path
    }

    #[test]
    fn prunes_backup_snapshots() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_full.bin"); // Backup
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_full.bin"); // Backup
        create_snapshot_file(tmp_dir.path(), "snapshot_3_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_4_0-of-1_full.bin"); // Backup
        create_snapshot_file(tmp_dir.path(), "snapshot_5_0-of-1_full.bin"); // Latest

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 5);

        snapshot_set.prune_backup_snapshots(3).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 5);

        snapshot_set.prune_backup_snapshots(1).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 3);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(3));
        assert_eq!(snapshot_set.snapshots[1].ordinal, SnapshotOrdinal(4));
        assert_eq!(snapshot_set.snapshots[2].ordinal, SnapshotOrdinal(5));

        // Construct a new SnapShotSet to verify that the files were actually deleted.
        drop(snapshot_set);
        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        snapshot_set.prune_backup_snapshots(0).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 2);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(3));
        assert_eq!(snapshot_set.snapshots[1].ordinal, SnapshotOrdinal(5));
    }

    #[test]
    fn prunes_not_completed_snapshots() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_3_0-of-1_pending.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_pending.bin"); // Not completed
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_full.bin"); // Not completed

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 3);

        snapshot_set.prune_not_completed_snapshots().unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 1);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(2));

        // Construct a new SnapShotSet to verify that the files were actually deleted.
        drop(snapshot_set);
        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 1);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(2));
    }
}
