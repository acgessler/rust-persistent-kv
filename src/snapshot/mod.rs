mod reader;
mod writer;

use prost::Message;

pub use reader::SnapshotReader;
pub use writer::{ SnapshotWriter, SnapshotWriterConfig };

#[derive(Clone, PartialEq, Message)]
pub struct SnapshotEntry {
    #[prost(bytes, tag = "1")]
    pub key: Vec<u8>,
    #[prost(bytes, tag = "2")]
    pub value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::{ config::SyncMode, snapshot::writer::SnapshotWriterConfig };

    use super::*;
    use tempfile::NamedTempFile;

    fn write_configs() -> Vec<SnapshotWriterConfig> {
        vec![
            SnapshotWriterConfig {
                sync_mode: SyncMode::SyncEveryWrite,
                use_positioned_writes: false,
            },
            SnapshotWriterConfig {
                sync_mode: SyncMode::SyncEveryWrite,
                use_positioned_writes: true,
            },
            SnapshotWriterConfig {
                sync_mode: SyncMode::NoExplicitSync,
                use_positioned_writes: false,
            },
            SnapshotWriterConfig {
                sync_mode: SyncMode::NoExplicitSync,
                use_positioned_writes: true,
            }
        ]
    }

    #[test]
    fn basic_io() {
        for config in write_configs() {
            let tmp_file = NamedTempFile::new().unwrap();
            let mut writer = SnapshotWriter::new(tmp_file.path(), false, config);
            writer.sequence_entry(b"foo", Some(b"1")).unwrap().commit().unwrap();
            writer.sequence_entry(b"bar", None).unwrap().commit().unwrap();
            writer.sequence_entry(b"baz", Some(b"2")).unwrap().commit().unwrap();
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
    }

    #[test]
    fn basic_large_data() {
        for config in write_configs() {
            let tmp_file = NamedTempFile::new().unwrap();
            let mut writer = SnapshotWriter::new(tmp_file.path(), false, config);
            let very_large_data = b"1".repeat(1000000);
            writer.sequence_entry(b"foo", Some(&very_large_data)).unwrap().commit().unwrap();
            writer.sequence_entry(b"bar", Some(&very_large_data)).unwrap().commit().unwrap();
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
}
