use prost::Message;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::config::SyncMode;

#[derive(Clone, PartialEq, Message)]
pub struct SnapshotEntry {
    #[prost(bytes, tag = "1")]
    pub key: Vec<u8>,
    #[prost(bytes, tag = "2")]
    pub value: Vec<u8>,
}

pub struct SnapshotWriter {
    file: Arc<File>,
    sync_mode: SyncMode,
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
                OpenOptions::new().write(true).append(append).create(true).open(path).unwrap()
            ),
            sync_mode,
        }
    }

    pub fn append_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>
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

pub struct SnapshotReader {
    file: File,
}

impl SnapshotReader {
    pub fn new(path: &Path) -> Self {
        Self {
            file: OpenOptions::new().read(true).open(path).unwrap(),
        }
    }

    pub fn read_entries<F>(&mut self, mut callback: F) -> Result<(), Box<dyn std::error::Error>>
        where F: FnMut(&SnapshotEntry)
    {
        // TODO(acgessler): Deal with broken/partly written entries.
        const CAP: usize = 1 << 16;
        let mut reader = BufReader::with_capacity(CAP, &self.file);
        let mut entry = SnapshotEntry::default();

        loop {
            let length = {
                let mut buffer = reader.fill_buf()?;
                if buffer.is_empty() {
                    break;
                }
                let len = buffer.len();
                entry.clear();
                entry.merge_length_delimited(&mut buffer)?;
                callback(&entry);
                len - buffer.len()
            };
            reader.consume(length);
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn read_entries_to_vec(
        &mut self
    ) -> Result<Vec<SnapshotEntry>, Box<dyn std::error::Error>> {
        let mut entries = Vec::new();
        self.read_entries(|entry| entries.push(entry.clone()))?;
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn basic_io() {
        let tmp_file = NamedTempFile::new().unwrap();
        {
            let mut writer = SnapshotWriter::new(tmp_file.path(), false, SyncMode::SyncEveryWrite);
            writer.append_entry(b"foo", Some(b"1")).unwrap();
            writer.append_entry(b"bar", None).unwrap();
        }
        {
            let mut reader = SnapshotReader::new(tmp_file.path());
            let entries = reader.read_entries_to_vec().unwrap();

            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].key, b"foo");
            assert_eq!(entries[0].value, b"1");
            assert_eq!(entries[1].key, b"bar");
            assert_eq!(entries[1].value, b"");
        }
    }
    // TODO: large data size
}
