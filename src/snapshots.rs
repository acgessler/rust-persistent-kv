use prost::Message;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;

#[derive(Clone, PartialEq, Message)]
pub struct SnapshotEntry {
    #[prost(bytes, tag = "1")]
    pub key: Vec<u8>,
    #[prost(bytes, tag = "2")]
    pub value: Vec<u8>,
}

pub struct SnapshotWriter {
    file: File,
    buffer: Vec<u8>,
    entry: SnapshotEntry,
}

impl SnapshotWriter {
    pub fn new(path: &Path, append: bool) -> Self {
        Self {
            file: OpenOptions::new().write(true).append(append).create(true).open(path).unwrap(),
            buffer: Vec::new(),
            entry: SnapshotEntry::default(),
        }
    }

    pub fn append_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Reuse entry and byte buffer memory to avoid unnecessary allocations
        self.entry.value.clear();
        if let Some(value) = value {
            self.entry.value.extend_from_slice(value);
        }
        self.entry.key.clear();
        self.entry.key.extend_from_slice(key);

        self.buffer.clear();
        self.entry.encode_length_delimited(&mut self.buffer)?;
        self.file.write_all(&self.buffer)?;

        // TODO(acgessler): Offer config option as to whether F_FULLFSYNC is required
        // or if loosing a few writes in a power-off scenario is acceptable.
        self.file.sync_all().unwrap();
        Ok(())
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
        const CAP: usize = 2048;
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
            let mut writer = SnapshotWriter::new(tmp_file.path(), false);
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
