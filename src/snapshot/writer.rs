use std::{
    cell::RefCell,
    fs::{ File, OpenOptions },
    io::Write,
    marker::PhantomData,
    path::Path,
    sync::{ atomic::{ AtomicU64, Ordering }, Arc, MutexGuard },
};

#[cfg(target_os = "windows")]
use std::os::windows::fs::FileExt;

use prost::Message;

use crate::config::SyncMode;

use super::SnapshotEntry;

#[derive(Clone, PartialEq, Debug)]
pub struct SnapshotWriterConfig {
    pub sync_mode: SyncMode,
    pub use_positioned_writes: bool,
}

pub struct SnapshotWriter {
    pub config: SnapshotWriterConfig,
    file: Arc<File>,
    next_offset: AtomicU64,
}

/// Returned by operations in `SnapshotWriter` and represents a sequenced
/// but not yet commited write operation. The operation can be commited
/// concurrently with other write operations on the same file, but the
/// commit() call must occur before any other sequence_entry() call on
/// the same thread (and Send is disallowed).
pub struct SequencedAppendOp {
    file: Arc<File>,
    should_sync: bool,
    should_write_at_offset: Option<(u64, usize)>,
    _is_not_send: PhantomData<MutexGuard<'static, ()>>,
}

impl SequencedAppendOp {
    pub fn commit(&mut self) -> std::io::Result<()> {
        if self.should_write_at_offset.is_some() {
            self.write_at_offset()?;
        }
        if self.should_sync {
            self.fsync();
        }
        Ok(())
    }

    fn should_commit(&self) -> bool {
        self.should_sync || self.should_write_at_offset.is_some()
    }

    fn fsync(&mut self) {
        self.should_sync = false;
        self.file.sync_data().unwrap();
    }

    fn write_at_offset(&mut self) -> std::io::Result<()> {
        let (offset, length) = self.should_write_at_offset.unwrap();
        self.should_write_at_offset = None;
        SnapshotWriter::BUFFER.with(|buffer| {
            let mut bytes_written = 0;
            while bytes_written < length {
                let bytes = self.file.seek_write(
                    &buffer.borrow_mut()[bytes_written..length],
                    offset + (bytes_written as u64)
                )?;
                bytes_written += bytes;
            }
            Ok(())
        })
    }
}

impl Drop for SequencedAppendOp {
    fn drop(&mut self) {
        if self.should_commit() {
            panic!("SequencedAppendOp was dropped without calling execute()");
        }
    }
}

impl SnapshotWriter {
    thread_local! {
        pub static BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        static ENTRY: RefCell<SnapshotEntry> = RefCell::new(SnapshotEntry::default());
    }

    pub fn new(path: &Path, append: bool, config: SnapshotWriterConfig) -> Self {
        let file = Arc::new(
            OpenOptions::new()
                .write(true)
                .append(if config.use_positioned_writes { false } else { append })
                .create(true)
                .open(path)
                .unwrap()
        );
        Self {
            file: file.clone(),
            config: config.clone(),
            next_offset: AtomicU64::new(
                if config.use_positioned_writes {
                    if append {
                        path.metadata().unwrap().len()
                    } else {
                        file.set_len(0).unwrap();
                        0
                    }
                } else {
                    0
                }
            ),
        }
    }

    pub fn sequence_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>
    ) -> Result<SequencedAppendOp, Box<dyn std::error::Error>> {
        // Reuse entry and byte buffer memory to avoid unnecessary allocations
        Self::ENTRY.with(|entry| {
            Self::BUFFER.with(|buffer| {
                let mut entry = entry.borrow_mut();
                let mut buffer = buffer.borrow_mut();
                entry.value.clear();
                if let Some(value) = value {
                    entry.value.extend_from_slice(value);
                }
                entry.key.clear();
                entry.key.extend_from_slice(key);

                buffer.clear();
                entry.encode_length_delimited(&mut *buffer)?;

                let should_write_at_offset = if self.config.use_positioned_writes {
                    Some((
                        self.next_offset.fetch_add(buffer.len() as u64, Ordering::SeqCst),
                        buffer.len(),
                    ))
                } else {
                    self.file.write_all(&buffer)?;
                    None
                };

                Ok(SequencedAppendOp {
                    file: self.file.clone(),
                    should_sync: self.config.sync_mode == SyncMode::SyncEveryWrite,
                    should_write_at_offset,
                    _is_not_send: PhantomData,
                })
            })
        })
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        self.file.sync_all().unwrap();
    }
}

// Tests: in mod.rs as they pair the writer with the reader.
