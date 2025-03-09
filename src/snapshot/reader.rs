use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read},
    path::Path,
};

use prost::Message;

use crate::snapshot::SnapshotEntry;

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
    where
        F: FnMut(&SnapshotEntry),
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
            let bytes_consumed_by_length_delim =
                (unsafe { prefix_buf.as_ptr().offset_from(buf[..].as_ptr()) }) as usize;

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
                    (message_length as i64) + (bytes_consumed_by_length_delim as i64)
                        - (bytes_already_read as i64),
                )?;
            }

            entry.clear();
            entry.merge(
                &buf[bytes_consumed_by_length_delim
                    ..bytes_consumed_by_length_delim + (message_length as usize)],
            )?;
            callback(&entry);
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn read_entries_to_vec(
        &mut self,
    ) -> Result<Vec<SnapshotEntry>, Box<dyn std::error::Error>> {
        let mut entries = Vec::new();
        self.read_entries(|entry| entries.push(entry.clone()))?;
        Ok(entries)
    }
}

// Tests: in mod.rs as they pair the writer with the reader.
