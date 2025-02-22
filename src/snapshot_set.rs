use std::{ fs::{ self, File }, io, path::{ Path, PathBuf } };
use regex::Regex;

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

#[derive(Clone, PartialEq, Debug)]
pub struct SnapshotInfo {
    pub snapshot_type: SnapshotType,
    pub ordinal: u64,
    pub path: PathBuf,
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
#[derive(Debug)]
pub struct SnapshotSet {
    pub snapshots: Vec<SnapshotInfo>,
    folder: PathBuf,
}

impl SnapshotSet {
    pub fn new(folder: &Path) -> Result<Self, &str> {
        // Scan the folder for all files matching snapshot pattern.
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(folder).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if let Some((ordinal, snapshot_type)) = Self::parse_snapshot_filename_(&path) {
                snapshots.push(SnapshotInfo {
                    snapshot_type,
                    ordinal,
                    path,
                });
            }
        }

        // Verify all ordinals are unique.
        let mut ordinals = snapshots
            .iter()
            .map(|snapshot| snapshot.ordinal)
            .collect::<Vec<_>>();
        ordinals.sort();
        ordinals.dedup();
        if ordinals.len() != snapshots.len() {
            return Err("Duplicate snapshot ordinals detected");
        }

        Ok(Self {
            folder: folder.to_path_buf(),
            snapshots,
        })
    }

    /// Registers a new snapshot path usable for the given snapshot type.
    /// This will return a new snapshot path that can be used to write the snapshot to.
    ///
    /// If `may_append_existing` is set to true, an existing snapshot file may be returned
    /// for SnapshotType::Diff only, if there is no full snapshot that came after it.
    ///
    /// A new snapshot file will have a ordinal higher than all previous snapshots and
    /// the file will be created with empty contents.
    pub fn create_or_get_snapshot(
        &mut self,
        snapshot_type: SnapshotType,
        may_append_existing: bool
    ) -> Result<SnapshotInfo, io::Error> { 
        if snapshot_type == SnapshotType::FullCompleted {
            return Err(
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Cannot create completed snapshot directly, use publish_completed_snapshot()"
                )
            );
        }
        if may_append_existing {
            let latest_diff_snapshot = self.snapshots
                .iter()
                .filter(|snapshot| snapshot.snapshot_type == snapshot_type)
                .max_by_key(|snapshot| snapshot.ordinal);
            let latest_full_snapshot = self.get_latest_full_snapshot();
            match (latest_diff_snapshot, latest_full_snapshot) {
                (Some(latest_diff_snapshot), Some(latest_full_snapshot)) if
                    latest_full_snapshot.ordinal < latest_diff_snapshot.ordinal
                => {
                    return Ok(latest_diff_snapshot.clone());
                }
                (Some(latest_diff_snapshot), None) => {
                    return Ok(latest_diff_snapshot.clone());
                }
                _ => (),
            }
        }
        // Should (and could) return &SnapshotInfo, but borrow checker doesn't follow the branches.
        self.create_new_snapshot_file_(snapshot_type).map(|e| e.clone())
    }


    /// Publishes a pending snapshot as a full snapshot. This will rename the snapshot file
    /// to indicate that it is a full snapshot and considered complete.
    pub fn publish_completed_snapshot(&mut self, pending_snapshot_ordinal: u64) -> Result<(), io::Error> {
        let pending_snapshot = self.snapshots
            .iter_mut()
            .find(|snapshot| snapshot.ordinal == pending_snapshot_ordinal)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Snapshot not found"))?;
        let new_snapshot_path = Self::generate_snapshot_filename_(
            pending_snapshot.ordinal,
            SnapshotType::FullCompleted
        );
        let new_snapshot_path = self.folder.join(new_snapshot_path);
        fs::rename(&pending_snapshot.path, &new_snapshot_path)?;
        pending_snapshot.path = new_snapshot_path;
        pending_snapshot.snapshot_type = SnapshotType::FullCompleted;
        Ok(())
    }

    /// Returns all snapshots that need to be restored in order to get the latest state.
    /// This includes the latest full snapshot and all differential snapshots since then.
    pub fn get_snapshots_to_restore(&self) -> Vec<&SnapshotInfo> {
        let mut snapshots_to_restore = Vec::new();
        let last_snapshot_ordinal = match self.get_latest_full_snapshot() {
            Some(snapshot) => {
                snapshots_to_restore.push(snapshot);
                snapshot.ordinal
            }
            None => 0,
        };
        snapshots_to_restore.append(&mut self.get_all_diff_snapshots_since(last_snapshot_ordinal));
        snapshots_to_restore
    }

    /// Returns the latest full snapshot that has been published and is considered complete.
    pub fn get_latest_full_snapshot(&self) -> Option<&SnapshotInfo> {
        self.snapshots
            .iter()
            .filter(|snapshot| snapshot.snapshot_type == SnapshotType::FullCompleted)
            .max_by_key(|snapshot| snapshot.ordinal)
    }

    /// Returns all differential snapshots that have been created since the snapshot
    /// with the given ordinal number. This is useful to determine which differential snapshots
    /// need to be applied to the full snapshot to get the latest state.
    pub fn get_all_diff_snapshots_since(&self, last_full_ordinal: u64) -> Vec<&SnapshotInfo> {
        self.snapshots
            .iter()
            .filter(
                |snapshot|
                    snapshot.snapshot_type == SnapshotType::Diff &&
                    snapshot.ordinal > last_full_ordinal
            )
            .collect()
    }

    /// Prunes backup snapshots, keeping only the latest `max_backups_keep` full snapshots.
    /// This is useful to limit the number of backup snapshots that are kept around.
    /// __Warning__: This will delete files from the file system.
    pub fn prune_backup_snapshots(
        &mut self,
        max_backups_keep: usize
    ) -> Result<(), std::io::Error> {
        let mut full_backup_snapshots = self.snapshots
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
            println!("Pruning backup snapshot: {:?}", snapshot.path);
            fs::remove_file(&snapshot.path)?;
            self.snapshots.retain(|s| s.path != snapshot.path);
        }
        Ok(())
    }

    pub fn prune_not_completed_snapshots(&mut self) -> Result<(), std::io::Error> {
        let not_completed_snapshots = self.snapshots
            .iter()
            .filter(|snapshot| snapshot.snapshot_type == SnapshotType::Pending)
            .cloned()
            .collect::<Vec<_>>();

        for snapshot in not_completed_snapshots.iter() {
            println!("Pruning not completed snapshot: {:?}", snapshot.path);
            fs::remove_file(&snapshot.path)?;
            self.snapshots.retain(|s| s.path != snapshot.path);
        }
        Ok(())
    }

    fn create_new_snapshot_file_(
        &mut self,
        snapshot_type: SnapshotType
    ) -> Result<&SnapshotInfo, io::Error> {
        let ordinal = self.get_next_ordinal_number_();
        let filename = Self::generate_snapshot_filename_(ordinal, snapshot_type);
        let path = self.folder.join(Path::new(&filename));
        File::create_new(path.clone())?;
        self.snapshots.push(SnapshotInfo {
            path: path.clone(),
            snapshot_type,
            ordinal,
        });
        Ok(self.snapshots.last().unwrap())
    }

    fn get_next_ordinal_number_(&self) -> u64 {
        self.snapshots
            .iter()
            .map(|snapshot| snapshot.ordinal)
            .max()
            .unwrap_or(0) + 1
    }

    fn generate_snapshot_filename_(ordinal: u64, snapshot_type: SnapshotType) -> String {
        let snapshot_type_str = match snapshot_type {
            SnapshotType::Diff => "diff",
            SnapshotType::FullCompleted => "full",
            SnapshotType::Pending => "pending",
        };
        format!("snapshot_{}_{}.bin", ordinal, snapshot_type_str)
    }

    fn parse_snapshot_filename_(path: &Path) -> Option<(u64, SnapshotType)> {
        let filename = path.file_name().unwrap().to_str().unwrap();
        let re = Regex::new(r"^snapshot_(\d+)_(diff|full|pending)\.bin$").unwrap();
        let captures = re.captures(filename)?;
        let ordinal = captures[1].parse().unwrap();
        let snapshot_type = match &captures[2] {
            "diff" => SnapshotType::Diff,
            "full" => SnapshotType::FullCompleted,
            "pending" => SnapshotType::Pending,
            _ => {
                return None;
            }
        };
        Some((ordinal, snapshot_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    fn create_temp_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn create_snapshot_file(folder: &Path, name: &str) -> PathBuf {
        let path = folder.join(name);
        File::create(&path).unwrap();
        path
    }

    #[test]
    fn empty() {
        let tmp_dir = create_temp_dir();

        let snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 0);
    }

    #[test]
    fn basic_detection() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_2_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_3_pending.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_4_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "other_file.txt");

        let snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 4);
        assert_eq!(snapshot_set.snapshots[0], SnapshotInfo {
            path: tmp_dir.path().join("snapshot_1_diff.bin"),
            ordinal: 1,
            snapshot_type: SnapshotType::Diff,
        });
        assert_eq!(snapshot_set.snapshots[1], SnapshotInfo {
            path: tmp_dir.path().join("snapshot_2_full.bin"),
            ordinal: 2,
            snapshot_type: SnapshotType::FullCompleted,
        });
        assert_eq!(snapshot_set.snapshots[2], SnapshotInfo {
            path: tmp_dir.path().join("snapshot_3_pending.bin"),
            ordinal: 3,
            snapshot_type: SnapshotType::Pending,
        });
        assert_eq!(snapshot_set.snapshots[3], SnapshotInfo {
            path: tmp_dir.path().join("snapshot_4_diff.bin"),
            ordinal: 4,
            snapshot_type: SnapshotType::Diff,
        });
    }

    #[test]
    fn fails_duplicate_ordinals() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_full.bin");

        let error = SnapshotSet::new(tmp_dir.path()).unwrap_err();
        assert_eq!(error, "Duplicate snapshot ordinals detected");
    }

    #[test]
    fn registers_new_snapshot_path_assigns_new_snapshot_ordinals() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_0_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_60_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_900000000000_pending.bin");

        let mut snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, false)
            .unwrap();

        assert_eq!(
            new_diff_snapshot_path.path,
            tmp_dir.path().join(PathBuf::from("snapshot_900000000001_diff.bin"))
        );

        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Pending, false)
            .unwrap();

        assert_eq!(
            new_diff_snapshot_path.path,
            tmp_dir.path().join(PathBuf::from("snapshot_900000000002_pending.bin"))
        );

        // Construct a new SnapShotSet to verify that the files were created on disk
        snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 5);
        assert_eq!(snapshot_set.snapshots[3].ordinal, 900000000001);
        assert_eq!(snapshot_set.snapshots[4].ordinal, 900000000002);
    }

    #[test]
    fn registers_new_snapshot_path_reuses_most_recent_diff_ordinal() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_0_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_2_diff.bin");

        let mut snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, true)
            .unwrap();

        assert_eq!(
            new_diff_snapshot_path.path,
            tmp_dir.path().join(PathBuf::from("snapshot_2_diff.bin"))
        );

        // Construct a new SnapShotSet to verify that no new files were created on disk
        snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 3);
    }

    #[test]
    fn registers_new_snapshot_path_from_empty() {
        let tmp_dir = create_temp_dir();

        let mut snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, true)
            .unwrap();

        // First ordinal number assigned is 1.
        assert_eq!(
            new_diff_snapshot_path.path,
            tmp_dir.path().join(PathBuf::from("snapshot_1_diff.bin"))
        );
    }

    #[test]
    fn registers_new_snapshot_path_fails_if_files_exist() {
        let tmp_dir = create_temp_dir();

        let mut snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();

        create_snapshot_file(&tmp_dir.path(), "snapshot_1_diff.bin");

        let error = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, true)
            .map_err(|e| e.kind());
        assert_eq!(error, Err(io::ErrorKind::AlreadyExists));
    }

    #[test]
    fn registers_new_snapshot_path_rejects_full_completed_type() {
        let tmp_dir = create_temp_dir();

        let mut snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();

        let error = snapshot_set
            .create_or_get_snapshot(SnapshotType::FullCompleted, true)
            .map_err(|e| e.kind());
        assert_eq!(error, Err(io::ErrorKind::InvalidInput));
    }

    #[test]
    fn gets_latest_full_snapshot() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_0_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_3_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_2_full.bin");

        let snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        let latest_full_snapshot = snapshot_set.get_latest_full_snapshot().unwrap();

        assert_eq!(latest_full_snapshot.ordinal, 3);
    }

    #[test]
    fn gets_all_diff_snapshots_since() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_2_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_3_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_4_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_5_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_9999_diff.bin");

        let snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        let latest_full_snapshot = snapshot_set.get_latest_full_snapshot().unwrap();
        let diff_snapshots = snapshot_set.get_all_diff_snapshots_since(
            latest_full_snapshot.ordinal
        );

        assert_eq!(diff_snapshots.len(), 2);
        assert_eq!(diff_snapshots[0].ordinal, 5);
        assert_eq!(diff_snapshots[1].ordinal, 9999);
    }

    #[test]
    fn gets_snapshots_to_restore() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_2_full.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_3_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_4_pending.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_5_diff.bin");

        let snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        let snapshots_to_restore = snapshot_set.get_snapshots_to_restore();

        assert_eq!(snapshots_to_restore.len(), 3);
        assert_eq!(snapshots_to_restore[0].ordinal, 2);
        assert_eq!(snapshots_to_restore[1].ordinal, 3);
        assert_eq!(snapshots_to_restore[2].ordinal, 5);
    }

    #[test]
    fn prunes_backup_snapshots() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_full.bin"); // Backup
        create_snapshot_file(&tmp_dir.path(), "snapshot_2_full.bin"); // Backup
        create_snapshot_file(&tmp_dir.path(), "snapshot_3_diff.bin");
        create_snapshot_file(&tmp_dir.path(), "snapshot_4_full.bin"); // Backup
        create_snapshot_file(&tmp_dir.path(), "snapshot_5_full.bin"); // Latest

        let mut snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 5);

        snapshot_set.prune_backup_snapshots(3).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 5);

        snapshot_set.prune_backup_snapshots(1).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 3);
        assert_eq!(snapshot_set.snapshots[0].ordinal, 3);
        assert_eq!(snapshot_set.snapshots[1].ordinal, 4);
        assert_eq!(snapshot_set.snapshots[2].ordinal, 5);

        // Construct a new SnapShotSet to verify that the files were actually deleted.
        snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        snapshot_set.prune_backup_snapshots(0).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 2);
        assert_eq!(snapshot_set.snapshots[0].ordinal, 3);
        assert_eq!(snapshot_set.snapshots[1].ordinal, 5);
    }

    #[test]
    fn prunes_not_completed_snapshots() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(&tmp_dir.path(), "snapshot_1_pending.bin"); // Not completed
        create_snapshot_file(&tmp_dir.path(), "snapshot_2_full.bin"); // Not completed
        create_snapshot_file(&tmp_dir.path(), "snapshot_3_pending.bin");

        let mut snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 3);

        snapshot_set.prune_not_completed_snapshots().unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 1);
        assert_eq!(snapshot_set.snapshots[0].ordinal, 2);

        // Construct a new SnapShotSet to verify that the files were actually deleted.
        snapshot_set = SnapshotSet::new(tmp_dir.path()).unwrap();

        assert_eq!(snapshot_set.snapshots.len(), 1);
        assert_eq!(snapshot_set.snapshots[0].ordinal, 2);
    }
}
