use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, Write},
    marker::PhantomData,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;
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
    file: WriterImpl,
    next_offset: AtomicU64,
}

impl SnapshotWriter {
    thread_local! {
        pub static BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        static ENTRY: RefCell<SnapshotEntry> = RefCell::new(SnapshotEntry::default());
    }

    /// Constructs a writer in the given path. If `append` is true, the writer will append to the
    /// file if it exists, otherwise it will immediately truncate the file.
    pub fn new(path: &Path, append: bool, config: SnapshotWriterConfig) -> Self {
        // Work around write_at issues on Linux by not using append when using positioned writes.
        let file = OpenOptions::new()
            .write(true)
            .append(if config.use_positioned_writes {
                false
            } else {
                append
            })
            .create(true)
            .open(path)
            .unwrap();
        let next_offset = AtomicU64::new(if config.use_positioned_writes {
            if append {
                path.metadata().unwrap().len()
            } else {
                file.set_len(0).unwrap(); // Explicitly truncate()
                0
            }
        } else {
            0
        });

        Self {
            file: if config.sync_mode == SyncMode::Buffered {
                WriterImpl::Buffered(Arc::new(Mutex::new(BufWriter::new(file))))
            } else {
                WriterImpl::Unbuffered(Arc::new(file))
            },
            config: config.clone(),
            next_offset,
        }
    }

    /// Invariant: each call to sequence_entry() must be matched by a call to commit() on the
    /// returned `SequencedAppendOp` before the next call to sequence_entry() on the same thread.
    /// This pattern must be followed regardless of configuration.
    pub fn sequence_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<SequencedAppendOp, Box<dyn std::error::Error>> {
        // Reuse entry and byte buffer memory to avoid unnecessary allocations
        Self::ENTRY.with_borrow_mut(|entry| {
            Self::BUFFER.with_borrow_mut(|buffer| {
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
                        self.next_offset
                            .fetch_add(buffer.len() as u64, Ordering::SeqCst),
                        buffer.len(),
                    ))
                } else {
                    match self.file {
                        WriterImpl::Buffered(ref file) => file.lock().unwrap().write_all(buffer)?,
                        WriterImpl::Unbuffered(ref mut file) => file.write_all(buffer)?,
                    }
                    None
                };

                Ok(SequencedAppendOp {
                    file: self.file.clone(),
                    should_sync: self.config.sync_mode == SyncMode::BlockAndSync,
                    should_write_at_offset,
                    _is_not_send: PhantomData,
                })
            })
        })
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        // Closing a file or flushing a buffer does not sync to disk, forcing
        // a sync is a desirable property of our store as it ensures that a
        // second store instance constructed after dropping the first will
        // read all data.
        self.file.fsync().unwrap();
    }
}

/// Returned by operations in `SnapshotWriter` and represents a sequenced
/// but not yet commited write operation. The operation can be commited
/// concurrently with other write operations on the same file, but the
/// commit() call must occur before any other sequence_entry() call on
/// the same thread (and Send is disallowed).
pub struct SequencedAppendOp {
    file: WriterImpl,
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
            self.fsync()?;
        }
        Ok(())
    }

    fn should_commit(&self) -> bool {
        self.should_sync || self.should_write_at_offset.is_some()
    }

    fn fsync(&mut self) -> std::io::Result<()> {
        self.should_sync = false;
        self.file.fsync()
    }

    fn write_at_offset(&mut self) -> std::io::Result<()> {
        let (offset, length) = self.should_write_at_offset.unwrap();
        self.should_write_at_offset = None;
        SnapshotWriter::BUFFER
            .with_borrow_mut(|buffer| self.file.seek_write_all(&buffer[..length], offset))
    }
}

impl Drop for SequencedAppendOp {
    fn drop(&mut self) {
        if self.should_commit() {
            panic!("SequencedAppendOp was dropped without calling execute()");
        }
    }
}

/// Internal abstraction to handle unsynced writes to a file and synced writes
/// to a buffered file, depending on configuration.
#[derive(Clone, Debug)]
enum WriterImpl {
    Buffered(Arc<Mutex<BufWriter<File>>>),
    Unbuffered(Arc<File>),
}

impl WriterImpl {
    fn fsync(&mut self) -> std::io::Result<()> {
        match self {
            WriterImpl::Buffered(file) => {
                let mut file = file.lock().unwrap();
                file.flush()?;
                file.get_ref().sync_data()
            }
            WriterImpl::Unbuffered(file) => file.sync_data(),
        }
    }

    fn seek_write_all(&self, buffer: &[u8], offset: u64) -> std::io::Result<()> {
        match self {
            WriterImpl::Buffered(file) => {
                let mut file = file.lock().unwrap();
                file.seek(std::io::SeekFrom::Start(offset))?;
                file.write_all(buffer)
            }
            WriterImpl::Unbuffered(file) => {
                let mut bytes_written = 0;
                let length = buffer.len();
                while bytes_written < length {
                    let bytes = Self::seek_write_(
                        file,
                        &buffer[bytes_written..length],
                        offset + (bytes_written as u64),
                    )?;
                    bytes_written += bytes;
                }
                Ok(())
            }
        }
    }

    #[cfg(target_os = "windows")]
    fn seek_write_(file: &File, buffer: &[u8], offset: u64) -> std::io::Result<usize> {
        file.seek_write(buffer, offset)
    }

    #[cfg(target_os = "linux")]
    fn seek_write_(file: &File, buffer: &[u8], offset: u64) -> std::io::Result<usize> {
        file.write_at(buffer, offset)
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    fn seek_write_(file: &File, buffer: &[u8], offset: u64) -> std::io::Result<usize> {
        use core::panic;
        panic!("Unsupported OS for seek_write");
    }
}

// Tests: in mod.rs as they pair the writer with the reader.
