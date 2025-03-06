use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
};

use prost::Message;

use crate::config::SyncMode;

use super::SnapshotEntry;

pub struct SnapshotWriter {
    file: Arc<File>,
    pub sync_mode: SyncMode,
}

pub struct SnapshotAppendOp {
    file: Arc<File>,
    should_sync: bool,
}

impl SnapshotAppendOp {
    fn fsync(&mut self) {
        self.should_sync = false;
        self.file.sync_data().unwrap();
    }
}

impl Drop for SnapshotAppendOp {
    fn drop(&mut self) {
        if self.should_sync {
            self.fsync();
        }
    }
}

impl SnapshotWriter {
    thread_local! {
        static BUFFER: Vec<u8> = const { Vec::new() };
        static ENTRY: SnapshotEntry = SnapshotEntry::default();
    }

    pub fn new(path: &Path, append: bool, sync_mode: SyncMode) -> Self {
        Self {
            file: Arc::new(
                OpenOptions::new()
                    .write(true)
                    .append(append)
                    .create(true)
                    .open(path)
                    .unwrap(),
            ),
            sync_mode,
        }
    }

    pub fn append_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<SnapshotAppendOp, Box<dyn std::error::Error>> {
        // Reuse entry and byte buffer memory to avoid unnecessary allocations
        let mut entry = Self::ENTRY.with(|entry| entry.clone());
        let mut buffer = Self::BUFFER.with(|buffer| buffer.clone());
        entry.value.clear();
        if let Some(value) = value {
            entry.value.extend_from_slice(value);
        }
        entry.key.clear();
        entry.key.extend_from_slice(key);

        buffer.clear();
        entry.encode_length_delimited(&mut buffer)?;
        self.file.write_all(&buffer)?;

        Ok(SnapshotAppendOp {
            file: self.file.clone(),
            should_sync: self.sync_mode == SyncMode::SyncEveryWrite,
        })
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        self.file.sync_all().unwrap();
    }
}
