use prost::Message;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::Read;
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
        const BUFFER_CAPACITY: usize = 1 << 16;
        const LENGTH_DELIMITER_LENGTH: usize = 10;

        let mut reader = BufReader::with_capacity(BUFFER_CAPACITY, &self.file);
        let mut entry = SnapshotEntry::default();
        let mut buf: Vec<u8> = Vec::new();

        loop {
            // Read up to 10 bytes to decode the length delimiter (varint).
            buf.clear();
            let bytes_already_read = reader
                .by_ref()
                .take(LENGTH_DELIMITER_LENGTH as u64)
                .read_to_end(&mut buf)?;
            if bytes_already_read == 0 {
                break;
            }
            let mut prefix_buf = &buf[..bytes_already_read];
            let message_length = prost::encoding::decode_varint(&mut prefix_buf)? as usize;
            // SAFETY: prefix_buf was advanced from the start of buf, so the offset is valid.
            let bytes_consumed_by_length_delim = (unsafe {
                prefix_buf.as_ptr().offset_from(buf[..].as_ptr())
            }) as usize;

            // Keep reading until we have the full message in buf. For very small messages
            // we have to backtrack, as the delimiter read may have read too much.
            let message_bytes_read = bytes_already_read - bytes_consumed_by_length_delim;
            if message_length > message_bytes_read {
                reader
                    .by_ref()
                    .take((message_length - message_bytes_read) as u64)
                    .read_to_end(&mut buf)?;
            } else if message_length + bytes_consumed_by_length_delim < bytes_already_read {
                reader.seek_relative(
                    (message_length as i64) +
                        (bytes_consumed_by_length_delim as i64) -
                        (bytes_already_read as i64)
                )?;
            }

            entry.clear();
            entry.merge(
                &buf
                    [
                        bytes_consumed_by_length_delim..bytes_consumed_by_length_delim +
                            (message_length as usize)
                    ]
            )?;
            callback(&entry);
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
        let mut writer = SnapshotWriter::new(tmp_file.path(), false, SyncMode::SyncEveryWrite);
        writer.append_entry(b"foo", Some(b"1")).unwrap();
        writer.append_entry(b"bar", None).unwrap();
        writer.append_entry(b"baz", Some(b"2")).unwrap();
        drop(writer);

        let mut reader = SnapshotReader::new(tmp_file.path());
        let entries = reader.read_entries_to_vec().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"foo");
        assert_eq!(entries[0].value, b"1");
        assert_eq!(entries[1].key, b"bar");
        assert_eq!(entries[1].value, b"");
        assert_eq!(entries[2].key, b"baz");
        assert_eq!(entries[2].value, b"2");
    }

    #[test]
    fn basic_large_data() {
        let tmp_file = NamedTempFile::new().unwrap();
        let mut writer = SnapshotWriter::new(tmp_file.path(), false, SyncMode::SyncEveryWrite);
        let very_large_data = b"1".repeat(1000000);
        writer.append_entry(b"foo", Some(&very_large_data)).unwrap();
        writer.append_entry(b"bar", Some(&very_large_data)).unwrap();
        drop(writer);

        let mut reader = SnapshotReader::new(tmp_file.path());
        let entries = reader.read_entries_to_vec().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, b"foo");
        assert_eq!(entries[0].value, very_large_data);
        assert_eq!(entries[1].key, b"bar");
        assert_eq!(entries[1].value, very_large_data);
    }
}
