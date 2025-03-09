use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

use regex::Regex;

use super::{SnapshotInfo, SnapshotOrdinal, SnapshotSet, SnapshotType};

/// Implementation of SnapshotSet using files on disk that exactly mirror the state
/// in memory, i.e., each entry in `snapshots` corresponds to a file in the folder, even
/// if the file is empty.
#[derive(Debug)]
pub struct FileSnapshotSet {
    pub snapshots: Vec<SnapshotInfo>,
    folder: PathBuf,
}

impl FileSnapshotSet {
    pub fn new(folder: &Path) -> Result<Self, io::Error> {
        fs::create_dir_all(folder)?;
        // Scan the folder for all files matching snapshot pattern and map them to SnapshotInfo.
        let mut snapshots: Vec<SnapshotInfo> = Vec::new();
        let mut seen_shards: HashSet<(SnapshotOrdinal, u64)> = HashSet::new();
        let mut info_by_ordinal: HashMap<SnapshotOrdinal, (u64, SnapshotType)> = HashMap::new();
        for entry in fs::read_dir(folder).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if let Some((ordinal, shard, shard_count, snapshot_type)) =
                Self::parse_snapshot_filename_(&path)
            {
                // Find existing snapshot and append this shard or create new one
                let snapshot = snapshots
                    .iter_mut()
                    .find(|snapshot| snapshot.ordinal == ordinal);
                if let Some(snapshot) = snapshot {
                    if seen_shards.contains(&(ordinal, shard)) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Duplicate snapshot shard detected",
                        ));
                    }
                    let (prior_shard_count, prior_snapshot_type) =
                        info_by_ordinal.get(&ordinal).unwrap();
                    if shard_count != *prior_shard_count || snapshot_type != *prior_snapshot_type {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Inconsistent snapshot shard count or type detected",
                        ));
                    }
                    snapshot.shard_paths.push(path);
                } else {
                    info_by_ordinal.insert(ordinal, (shard_count, snapshot_type));
                    snapshots.push(SnapshotInfo {
                        snapshot_type,
                        ordinal,
                        shard_paths: vec![path],
                    });
                }
                seen_shards.insert((ordinal, shard));
            }
        }

        // Validate that all shards are present for each ordinal.
        for snapshot in snapshots.iter() {
            let (shard_count, _) = info_by_ordinal.get(&snapshot.ordinal).unwrap();
            if snapshot.shard_paths.len() as u64 != *shard_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Missing snapshot shard detected",
                ));
            }
        }

        // Sort data by (ordinal, shard-index)
        snapshots.sort_by_key(|snapshot| snapshot.ordinal);
        snapshots
            .iter_mut()
            .for_each(|snapshot| snapshot.shard_paths.sort());

        Ok(Self {
            folder: folder.to_path_buf(),
            snapshots,
        })
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
    pub fn get_all_diff_snapshots_since(
        &self,
        last_full_ordinal: SnapshotOrdinal,
    ) -> Vec<&SnapshotInfo> {
        self.snapshots
            .iter()
            .filter(|snapshot| {
                snapshot.snapshot_type == SnapshotType::Diff && snapshot.ordinal > last_full_ordinal
            })
            .collect()
    }

    fn create_new_snapshot_file_(
        &mut self,
        snapshot_type: SnapshotType,
        shard_count: u64,
    ) -> Result<&SnapshotInfo, io::Error> {
        let ordinal = self.get_next_ordinal_number_();
        let mut shard_paths = Vec::new();
        for shard in 0..shard_count {
            let filename =
                Self::generate_snapshot_filename_(ordinal, shard, shard_count, snapshot_type);
            let path = self.folder.join(Path::new(&filename));
            File::create_new(path.clone())?;
            shard_paths.push(path);
        }
        self.snapshots.push(SnapshotInfo {
            shard_paths,
            snapshot_type,
            ordinal,
        });
        Ok(self.snapshots.last().unwrap())
    }

    fn get_next_ordinal_number_(&self) -> SnapshotOrdinal {
        SnapshotOrdinal(
            self.snapshots
                .iter()
                .map(|snapshot| snapshot.ordinal.0)
                .max()
                .unwrap_or(0)
                + 1,
        )
    }

    fn generate_snapshot_filename_(
        ordinal: SnapshotOrdinal,
        shard: u64,
        shard_count: u64,
        snapshot_type: SnapshotType,
    ) -> String {
        let snapshot_type_str = match snapshot_type {
            SnapshotType::Diff => "diff",
            SnapshotType::FullCompleted => "full",
            SnapshotType::Pending => "pending",
        };
        format!(
            "snapshot_{}_{}-of-{}_{}.bin",
            ordinal.0, shard, shard_count, snapshot_type_str
        )
    }

    fn parse_snapshot_filename_(path: &Path) -> Option<(SnapshotOrdinal, u64, u64, SnapshotType)> {
        let filename = path.file_name().unwrap().to_str().unwrap();
        let re = Regex::new(r"^snapshot_(\d+)_(\d+)-of-(\d+)_(diff|full|pending)\.bin$").unwrap();
        let captures = re.captures(filename)?;
        let ordinal = SnapshotOrdinal(captures[1].parse().unwrap());
        let shard = captures[2].parse().unwrap();
        let shard_count = captures[3].parse().unwrap();
        let snapshot_type = match &captures[4] {
            "diff" => SnapshotType::Diff,
            "full" => SnapshotType::FullCompleted,
            "pending" => SnapshotType::Pending,
            _ => {
                return None;
            }
        };
        Some((ordinal, shard, shard_count, snapshot_type))
    }
}

impl SnapshotSet for FileSnapshotSet {
    fn create_or_get_snapshot(
        &mut self,
        snapshot_type: SnapshotType,
        shard_count: u64,
        may_append_existing: bool,
    ) -> Result<SnapshotInfo, io::Error> {
        assert!(shard_count > 0);
        assert!(
            snapshot_type != SnapshotType::FullCompleted,
            "Cannot create completed snapshot directly, use publish_completed_snapshot()"
        );
        if may_append_existing {
            let latest_diff_snapshot = self
                .snapshots
                .iter()
                .filter(|snapshot| snapshot.snapshot_type == snapshot_type)
                .max_by_key(|snapshot| snapshot.ordinal);
            let latest_full_snapshot = self.get_latest_full_snapshot();
            // We cannot continue writing the existing files if the shard count changed,
            // this happens if the binary is restarted with different configuration.
            if let Some(latest_diff_snapshot) = latest_diff_snapshot {
                if latest_diff_snapshot.shard_paths.len() as u64 == shard_count {
                    if let Some(latest_full_snapshot) = latest_full_snapshot {
                        if latest_full_snapshot.ordinal < latest_diff_snapshot.ordinal {
                            return Ok(latest_diff_snapshot.clone());
                        }
                    } else {
                        return Ok(latest_diff_snapshot.clone());
                    }
                }
            }
        }
        // Should (and could) return &SnapshotInfo, but borrow checker doesn't follow the branches.
        self.create_new_snapshot_file_(snapshot_type, shard_count)
            .cloned()
    }

    fn publish_completed_snapshot(
        &mut self,
        pending_snapshot_ordinal: SnapshotOrdinal,
        purge_obsolete_diff_snapshots: bool,
        purge_obsolete_pending_snapshots: bool,
    ) -> Result<(), io::Error> {
        let pending_snapshot = self
            .snapshots
            .iter_mut()
            .find(|snapshot| snapshot.ordinal == pending_snapshot_ordinal)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Snapshot not found"))?;
        if pending_snapshot.snapshot_type != SnapshotType::Pending {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Snapshot is not pending",
            ));
        }
        let shard_count = pending_snapshot.shard_paths.len() as u64;
        for shard in 0..shard_count {
            let new_snapshot_path = Self::generate_snapshot_filename_(
                pending_snapshot.ordinal,
                shard,
                shard_count,
                SnapshotType::FullCompleted,
            );
            let new_snapshot_path = self.folder.join(new_snapshot_path);
            fs::rename(
                &pending_snapshot.shard_paths[shard as usize],
                &new_snapshot_path,
            )?;
            pending_snapshot.shard_paths[shard as usize] = new_snapshot_path;
        }
        pending_snapshot.snapshot_type = SnapshotType::FullCompleted;

        if purge_obsolete_diff_snapshots || purge_obsolete_pending_snapshots {
            let obsolete_snapshot: Vec<_> = self
                .snapshots
                .iter()
                .filter(|snapshot| {
                    ((purge_obsolete_diff_snapshots
                        && snapshot.snapshot_type == SnapshotType::Diff)
                        || (purge_obsolete_pending_snapshots
                            && snapshot.snapshot_type == SnapshotType::Pending))
                        && snapshot.ordinal < pending_snapshot_ordinal
                })
                .cloned()
                .collect();
            for obsolete_snapshot in obsolete_snapshot {
                for path in obsolete_snapshot.shard_paths.iter() {
                    fs::remove_file(path)?;
                }
                self.snapshots
                    .retain(|s| s.ordinal != obsolete_snapshot.ordinal);
            }
        }
        Ok(())
    }

    fn get_snapshots_to_restore(&self) -> Vec<&SnapshotInfo> {
        let mut snapshots_to_restore = Vec::new();
        let last_snapshot_ordinal = match self.get_latest_full_snapshot() {
            Some(snapshot) => {
                snapshots_to_restore.push(snapshot);
                snapshot.ordinal
            }
            None => SnapshotOrdinal(0),
        };
        snapshots_to_restore.append(&mut self.get_all_diff_snapshots_since(last_snapshot_ordinal));
        snapshots_to_restore
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

        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 0);
    }

    #[test]
    fn empty_folder_does_not_exist_yet() {
        let tmp_dir = create_temp_dir();

        let snapshot_set = FileSnapshotSet::new(&tmp_dir.path().join("new-folder")).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 0);
    }

    #[test]
    fn snapshots_in_ordinal_order() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_4_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_3_0-of-1_pending.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "other_file.txt");

        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 4);
        assert_eq!(
            snapshot_set.snapshots[0],
            SnapshotInfo {
                shard_paths: vec![tmp_dir.path().join("snapshot_1_0-of-1_diff.bin")],
                ordinal: SnapshotOrdinal(1),
                snapshot_type: SnapshotType::Diff,
            }
        );
        assert_eq!(
            snapshot_set.snapshots[1],
            SnapshotInfo {
                shard_paths: vec![tmp_dir.path().join("snapshot_2_0-of-1_full.bin")],
                ordinal: SnapshotOrdinal(2),
                snapshot_type: SnapshotType::FullCompleted,
            }
        );
        assert_eq!(
            snapshot_set.snapshots[2],
            SnapshotInfo {
                shard_paths: vec![tmp_dir.path().join("snapshot_3_0-of-1_pending.bin")],
                ordinal: SnapshotOrdinal(3),
                snapshot_type: SnapshotType::Pending,
            }
        );
        assert_eq!(
            snapshot_set.snapshots[3],
            SnapshotInfo {
                shard_paths: vec![tmp_dir.path().join("snapshot_4_0-of-1_diff.bin")],
                ordinal: SnapshotOrdinal(4),
                snapshot_type: SnapshotType::Diff,
            }
        );
    }

    #[test]
    fn snapshots_in_shard_order() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_1-of-3_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_2-of-3_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-3_diff.bin");

        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 1);
        assert_eq!(snapshot_set.snapshots[0].shard_paths.len(), 3);
        assert_eq!(
            snapshot_set.snapshots[0].shard_paths[0],
            tmp_dir.path().join("snapshot_1_0-of-3_diff.bin")
        );
        assert_eq!(
            snapshot_set.snapshots[0].shard_paths[1],
            tmp_dir.path().join("snapshot_1_1-of-3_diff.bin")
        );
        assert_eq!(
            snapshot_set.snapshots[0].shard_paths[2],
            tmp_dir.path().join("snapshot_1_2-of-3_diff.bin")
        );
    }

    #[test]
    fn fails_duplicate_shards() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_full.bin");

        let error = FileSnapshotSet::new(tmp_dir.path()).unwrap_err();
        assert_eq!(error.to_string(), "Duplicate snapshot shard detected");
    }

    #[test]
    fn fails_missing_shards() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_1-of-2_diff.bin");

        let error = FileSnapshotSet::new(tmp_dir.path()).unwrap_err();
        assert_eq!(error.to_string(), "Missing snapshot shard detected");
    }

    #[test]
    fn fails_mismatched_shard_counts() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_1-of-2_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_full.bin");

        let error = FileSnapshotSet::new(tmp_dir.path()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Inconsistent snapshot shard count or type detected"
        );
    }

    #[test]
    fn fails_mismatched_shard_types() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_1-of-2_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-2_diff.bin");

        let error = FileSnapshotSet::new(tmp_dir.path()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Inconsistent snapshot shard count or type detected"
        );
    }

    #[test]
    fn registers_new_snapshot_path_assigns_new_snapshot_ordinals() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_0_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_60_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_900000000000_0-of-1_pending.bin");

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, 2, false)
            .unwrap();

        assert_eq!(new_diff_snapshot_path.shard_paths.len(), 2);
        assert_eq!(
            new_diff_snapshot_path.shard_paths[0],
            tmp_dir.path().join("snapshot_900000000001_0-of-2_diff.bin")
        );
        assert_eq!(
            new_diff_snapshot_path.shard_paths[1],
            tmp_dir.path().join("snapshot_900000000001_1-of-2_diff.bin")
        );

        let new_pending_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Pending, 2, false)
            .unwrap();

        assert_eq!(new_pending_snapshot_path.shard_paths.len(), 2);
        assert_eq!(
            new_pending_snapshot_path.shard_paths[0],
            tmp_dir
                .path()
                .join("snapshot_900000000002_0-of-2_pending.bin")
        );
        assert_eq!(
            new_pending_snapshot_path.shard_paths[1],
            tmp_dir
                .path()
                .join("snapshot_900000000002_1-of-2_pending.bin")
        );

        // Construct a new SnapShotSet to verify that the files were created on disk
        snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 5);
        assert_eq!(
            snapshot_set.snapshots[3].ordinal,
            SnapshotOrdinal(900000000001)
        );
        assert_eq!(
            snapshot_set.snapshots[4].ordinal,
            SnapshotOrdinal(900000000002)
        );
        assert_eq!(snapshot_set.snapshots[3].shard_paths.len(), 2);
        assert_eq!(snapshot_set.snapshots[3].shard_paths.len(), 2);
    }

    #[test]
    fn registers_new_snapshot_path_reuses_most_recent_diff_ordinal() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_0_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_diff.bin");

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, 1, true)
            .unwrap();

        assert_eq!(
            new_diff_snapshot_path.single_shard_path(),
            tmp_dir.path().join("snapshot_2_0-of-1_diff.bin")
        );

        // Construct a new SnapShotSet to verify that no new files were created on disk
        snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 3);
    }

    #[test]
    fn registers_new_snapshot_path_does_not_reuse_mismatching_shard_count() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_0_0-of-2_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_0_1-of-2_diff.bin");

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, 1, true)
            .unwrap();

        assert_eq!(
            new_diff_snapshot_path.single_shard_path(),
            tmp_dir.path().join("snapshot_1_0-of-1_diff.bin")
        );
    }

    #[test]
    fn registers_new_snapshot_path_from_empty() {
        let tmp_dir = create_temp_dir();

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        let new_diff_snapshot_path = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, 1, true)
            .unwrap();

        // First ordinal number assigned is 1.
        assert_eq!(
            new_diff_snapshot_path.single_shard_path(),
            tmp_dir.path().join("snapshot_1_0-of-1_diff.bin")
        );
    }

    #[test]
    fn registers_new_snapshot_path_fails_if_files_exist() {
        let tmp_dir = create_temp_dir();

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();

        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_diff.bin");

        let error = snapshot_set
            .create_or_get_snapshot(SnapshotType::Diff, 1, true)
            .map_err(|e| e.kind());
        assert_eq!(error, Err(io::ErrorKind::AlreadyExists));
    }

    #[test]
    #[should_panic(
        expected = "Cannot create completed snapshot directly, use publish_completed_snapshot()"
    )]
    fn registers_new_snapshot_path_rejects_full_completed_type() {
        let tmp_dir = create_temp_dir();

        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();

        snapshot_set
            .create_or_get_snapshot(SnapshotType::FullCompleted, 1, true)
            .unwrap();
    }

    #[test]
    fn gets_latest_full_snapshot() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_0_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_3_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_full.bin");

        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        let latest_full_snapshot = snapshot_set.get_latest_full_snapshot().unwrap();

        assert_eq!(latest_full_snapshot.ordinal, SnapshotOrdinal(3));
    }

    #[test]
    fn gets_all_diff_snapshots_since() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_3_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_4_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_5_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_9999_0-of-1_diff.bin");

        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        let latest_full_snapshot = snapshot_set.get_latest_full_snapshot().unwrap();
        let diff_snapshots =
            snapshot_set.get_all_diff_snapshots_since(latest_full_snapshot.ordinal);

        assert_eq!(diff_snapshots.len(), 2);
        assert_eq!(diff_snapshots[0].ordinal, SnapshotOrdinal(5));
        assert_eq!(diff_snapshots[1].ordinal, SnapshotOrdinal(9999));
    }

    #[test]
    fn publishes_completed_snapshot() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_diff.bin"); // Incorporated into snapshot
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_diff.bin"); // Incorporated into snapshot
        create_snapshot_file(tmp_dir.path(), "snapshot_3_0-of-1_pending.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_4_0-of-1_diff.bin"); // Created after snapshot cut-off
        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();

        snapshot_set
            .publish_completed_snapshot(SnapshotOrdinal(3), true, false)
            .unwrap();

        // Verify that the existing snapshot set reflects the correct change.
        assert_eq!(snapshot_set.snapshots.len(), 2);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(3));
        assert_eq!(
            snapshot_set.snapshots[0].snapshot_type,
            SnapshotType::FullCompleted
        );
        assert_eq!(snapshot_set.snapshots[1].ordinal, SnapshotOrdinal(4));

        // Construct a new SnapShotSet to verify that the file changes actually hit disk.
        snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 2);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(3));
        assert_eq!(
            snapshot_set.snapshots[0].snapshot_type,
            SnapshotType::FullCompleted
        );
        assert_eq!(snapshot_set.snapshots[1].ordinal, SnapshotOrdinal(4));
    }

    #[test]
    fn publishes_completed_snapshot_purges_prior_pending() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_pending.bin"); // Prior abandoned
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_pending.bin");
        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();

        snapshot_set
            .publish_completed_snapshot(SnapshotOrdinal(2), false, true)
            .unwrap();

        // Verify that the existing snapshot set reflects the correct change.
        assert_eq!(snapshot_set.snapshots.len(), 1);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(2));

        // Construct a new SnapShotSet to verify that the file changes actually hit disk.
        snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        assert_eq!(snapshot_set.snapshots.len(), 1);
        assert_eq!(snapshot_set.snapshots[0].ordinal, SnapshotOrdinal(2));
    }

    #[test]
    fn publishes_completed_snapshot_already_published() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_full.bin");
        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();

        let error = snapshot_set
            .publish_completed_snapshot(SnapshotOrdinal(1), true, true)
            .map_err(|e| e.kind());

        assert_eq!(error, Err(io::ErrorKind::AlreadyExists));
    }

    #[test]
    fn publishes_completed_snapshot_not_found() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_full.bin");
        let mut snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();

        let error = snapshot_set
            .publish_completed_snapshot(SnapshotOrdinal(2), true, true)
            .map_err(|e| e.kind());

        assert_eq!(error, Err(io::ErrorKind::NotFound));
    }

    #[test]
    fn gets_snapshots_to_restore() {
        let tmp_dir = create_temp_dir();
        create_snapshot_file(tmp_dir.path(), "snapshot_1_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_2_0-of-1_full.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_3_0-of-1_diff.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_4_0-of-1_pending.bin");
        create_snapshot_file(tmp_dir.path(), "snapshot_5_0-of-1_diff.bin");

        let snapshot_set = FileSnapshotSet::new(tmp_dir.path()).unwrap();
        let snapshots_to_restore = snapshot_set.get_snapshots_to_restore();

        assert_eq!(snapshots_to_restore.len(), 3);
        assert_eq!(snapshots_to_restore[0].ordinal, SnapshotOrdinal(2));
        assert_eq!(snapshots_to_restore[1].ordinal, SnapshotOrdinal(3));
        assert_eq!(snapshots_to_restore[2].ordinal, SnapshotOrdinal(5));
    }
}
