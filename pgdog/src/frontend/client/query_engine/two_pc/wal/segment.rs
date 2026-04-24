//! Two-phase commit WAL segment files.
//!
//! Each segment is a single file `<start_lsn:020>.wal` in the WAL
//! directory. It begins with a 16-byte header (a 4-byte magic followed
//! by 12 reserved bytes) and is followed by a stream of records as
//! described in [`super::record`].
//!
//! [`Segment`] is the writable handle over an open segment file; it is
//! intentionally not `Send`-shared and is owned by a single writer task.
//! Records are appended one at a time with [`Segment::append`]; durability
//! is achieved by calling [`Segment::sync`] separately, which lets a
//! caller batch many appends behind a single fsync (group commit).
//!
//! [`SegmentReader`] iterates an existing segment one record at a time.
//! It tracks the byte offset of the last good record so that a torn or
//! corrupt tail can be truncated when a reader is converted into a
//! writable segment via [`Segment::from_reader`].
//!
//! # Durability
//!
//! [`Segment::sync`] calls `tokio::fs::File::sync_all`, which on Linux
//! issues `fsync(2)` and provides true durability. On macOS, `fsync(2)`
//! flushes only kernel buffers and does not guarantee that data has
//! reached the physical device; true durability there requires
//! `F_FULLFSYNC`, which is not exposed by tokio.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use bytes::{Buf, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tracing::warn;

use super::error::Error;
use super::record::Record;

const MAGIC: &[u8; 4] = b"PG2W";
const HEADER_BYTES: u64 = 16;
const READ_CHUNK: usize = 64 * 1024;
const FILE_SUFFIX: &str = ".wal";

/// Format a segment filename for the given start LSN.
fn segment_filename(start_lsn: u64) -> String {
    format!("{:020}{}", start_lsn, FILE_SUFFIX)
}

/// Parse a segment filename's start LSN.
fn parse_segment_filename(name: &str) -> Result<u64, Error> {
    let stem = name
        .strip_suffix(FILE_SUFFIX)
        .ok_or_else(|| Error::BadSegmentName(name.to_string()))?;
    u64::from_str(stem).map_err(|_| Error::BadSegmentName(name.to_string()))
}

/// Delete every segment in `dir` whose contents lie strictly before
/// `lsn` — i.e. segments whose successor's start LSN is `<= lsn`. The
/// segment that *contains* `lsn` is preserved because it still has live
/// records after the checkpoint that future recovery needs to replay.
/// Per-file deletion errors are logged but don't abort the GC.
pub async fn gc_before_lsn(dir: &Path, lsn: u64) -> Result<(), Error> {
    let segments = list_segments(dir).await?;
    let mut last_kept = None;
    for (i, path) in segments.iter().enumerate() {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Error::BadSegmentName(path.display().to_string()))?;
        if parse_segment_filename(name)? > lsn {
            break;
        }
        last_kept = Some(i);
    }
    let Some(keep) = last_kept else {
        return Ok(());
    };
    for path in &segments[..keep] {
        if let Err(err) = tokio::fs::remove_file(path).await {
            warn!(
                "[2pc] could not delete old wal segment {}: {}",
                path.display(),
                err
            );
        }
    }
    Ok(())
}

/// List the WAL segment files in `dir`, sorted ascending by start LSN.
///
/// Files in `dir` whose names don't parse as a segment are skipped with a
/// warning. The returned vec is empty if the directory contains no
/// segments yet.
pub async fn list_segments(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut segments: Vec<(u64, PathBuf)> = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(os) => {
                warn!("skipping non-utf8 entry in wal dir: {:?}", os);
                continue;
            }
        };
        if !name.ends_with(FILE_SUFFIX) {
            continue;
        }
        match parse_segment_filename(&name) {
            Ok(lsn) => segments.push((lsn, entry.path())),
            Err(err) => warn!("skipping malformed segment filename {}: {}", name, err),
        }
    }
    segments.sort_by_key(|(lsn, _)| *lsn);
    Ok(segments.into_iter().map(|(_, p)| p).collect())
}

/// Iterates records out of an existing segment file.
///
/// `next` returns one record at a time, reading from the file in chunks.
/// Each terminal condition is reported explicitly:
///
/// - `Ok(Some(record))` — a record was decoded; iteration continues.
/// - `Ok(None)` — clean end of stream (EOF with no leftover bytes).
/// - `Err(Error::TornTail { .. })` — file ends mid-record. Normal at
///   the last segment after a crash; suspicious anywhere else.
/// - `Err(Error::Crc | InvalidTag | EmptyRecord | Decode)` — record
///   framing or payload is corrupt.
/// - `Err(Error::Io(_))` — disk-side IO error.
///
/// `last_good_offset` always points at the end of the last successfully
/// decoded record so [`SegmentReader::into_writable`] can truncate the
/// file there regardless of how iteration terminated.
///
/// After any terminal condition, subsequent `next` calls return
/// `Ok(None)` (the error is reported once).
#[derive(Debug)]
pub struct SegmentReader {
    file: File,
    path: PathBuf,
    start_lsn: u64,
    next_lsn: u64,
    /// Bytes that have been read from the file but not yet decoded.
    buf: BytesMut,
    /// File offset (in bytes) of the end of the last successfully decoded
    /// record, or `HEADER_BYTES` if no records have been decoded yet.
    last_good_offset: u64,
    /// True once iteration has terminated; further `next` calls return
    /// `Ok(None)`.
    done: bool,
}

impl SegmentReader {
    pub async fn open(path: &Path) -> Result<Self, Error> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Error::BadSegmentName(path.display().to_string()))?;
        let start_lsn = parse_segment_filename(name)?;

        let mut file = OpenOptions::new().read(true).write(true).open(path).await?;

        let mut header = [0u8; HEADER_BYTES as usize];
        match file.read_exact(&mut header).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::BadSegmentHeader);
            }
            Err(err) => return Err(err.into()),
        }
        if &header[..MAGIC.len()] != MAGIC {
            return Err(Error::BadSegmentHeader);
        }

        Ok(Self {
            file,
            path: path.to_path_buf(),
            start_lsn,
            next_lsn: start_lsn,
            buf: BytesMut::new(),
            last_good_offset: HEADER_BYTES,
            done: false,
        })
    }

    pub fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    /// LSN of the next record this reader will return.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// File offset (in bytes) of the end of the last good record.
    pub fn last_good_offset(&self) -> u64 {
        self.last_good_offset
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Decode and return the next record. See the type-level docs for
    /// the full set of return values.
    pub async fn next(&mut self) -> Result<Option<Record>, Error> {
        if self.done {
            return Ok(None);
        }
        loop {
            match Record::decode(&self.buf) {
                Ok(Some(d)) => {
                    let consumed = d.consumed;
                    let record = d.record;
                    self.buf.advance(consumed);
                    self.last_good_offset += consumed as u64;
                    self.next_lsn += 1;
                    return Ok(Some(record));
                }
                Ok(None) => {
                    let read_more = self.read_chunk().await?;
                    if !read_more {
                        self.done = true;
                        if self.buf.is_empty() {
                            return Ok(None);
                        }
                        return Err(Error::TornTail {
                            offset: self.last_good_offset,
                            unconsumed: self.buf.len(),
                        });
                    }
                }
                Err(err) => {
                    self.done = true;
                    return Err(err);
                }
            }
        }
    }

    /// Consume this reader and turn it into a writable [`Segment`],
    /// truncating any torn or corrupt tail at `last_good_offset`. The
    /// reader's underlying file handle is reused.
    ///
    /// The reader should already have been drained by repeated calls to
    /// [`SegmentReader::next`]; if it hasn't, records past
    /// `last_good_offset` will be lost.
    pub async fn into_writable(self) -> Result<Segment, Error> {
        let SegmentReader {
            mut file,
            path,
            start_lsn,
            next_lsn,
            last_good_offset,
            ..
        } = self;

        let on_disk = file.metadata().await?.len();
        if on_disk > last_good_offset {
            file.set_len(last_good_offset).await?;
            file.sync_all().await?;
        }
        file.seek(SeekFrom::Start(last_good_offset)).await?;

        Ok(Segment {
            file,
            path,
            start_lsn,
            next_lsn,
            size_bytes: last_good_offset,
        })
    }

    /// Read up to `READ_CHUNK` bytes into `buf`. Returns `Ok(false)` if
    /// the read returned 0 bytes (EOF), `Ok(true)` if any bytes were read.
    async fn read_chunk(&mut self) -> Result<bool, Error> {
        let old_len = self.buf.len();
        self.buf.resize(old_len + READ_CHUNK, 0);
        let n = self.file.read(&mut self.buf[old_len..]).await?;
        self.buf.truncate(old_len + n);
        Ok(n > 0)
    }
}

/// A writable WAL segment.
///
/// Owned by a single writer task. Records are appended via
/// [`Segment::append`]; durability is requested separately via
/// [`Segment::sync`] so a caller can batch multiple appends behind one
/// fsync (group commit).
#[derive(Debug)]
pub struct Segment {
    file: File,
    path: PathBuf,
    start_lsn: u64,
    next_lsn: u64,
    size_bytes: u64,
}

impl Segment {
    /// Create a new empty segment file in `dir`, write the header, and fsync.
    pub async fn create(dir: &Path, start_lsn: u64) -> Result<Self, Error> {
        let path = dir.join(segment_filename(start_lsn));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .await?;

        let mut header = [0u8; HEADER_BYTES as usize];
        header[..MAGIC.len()].copy_from_slice(MAGIC);
        file.write_all(&header).await?;
        file.sync_all().await?;

        Ok(Self {
            file,
            path,
            start_lsn,
            next_lsn: start_lsn,
            size_bytes: HEADER_BYTES,
        })
    }

    /// Append a pre-encoded batch of `records` records to the segment without
    /// syncing. The caller must call [`Segment::sync`] when durability is
    /// required for prior appends. Returns the LSN assigned to the first
    /// record in the batch; subsequent records have LSNs `start + i`.
    pub async fn append_batch(&mut self, encoded: &[u8], records: u32) -> Result<u64, Error> {
        self.file.write_all(encoded).await?;
        let start = self.next_lsn;
        self.next_lsn += records as u64;
        self.size_bytes += encoded.len() as u64;
        Ok(start)
    }

    /// Flush all previously appended bytes to durable storage.
    pub async fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_all().await?;
        Ok(())
    }

    pub fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    /// LSN that will be assigned to the next [`Segment::append`] call.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Total file size on disk, including the header.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::super::record::{BeginPayload, TxnPayload};
    use super::*;
    use crate::frontend::client::query_engine::two_pc::TwoPcTransaction;
    use tempfile::TempDir;

    #[test]
    fn filename_round_trip() {
        for lsn in [0u64, 1, 16, 1234, u64::MAX] {
            let name = segment_filename(lsn);
            assert_eq!(parse_segment_filename(&name).unwrap(), lsn);
            assert_eq!(name.len(), 24);
        }
    }

    #[test]
    fn bad_segment_name_rejected() {
        assert!(parse_segment_filename("not-a-segment.txt").is_err());
        assert!(parse_segment_filename("xyz.wal").is_err());
    }

    #[tokio::test]
    async fn create_then_reopen_empty() {
        let dir = TempDir::new().unwrap();
        let seg = Segment::create(dir.path(), 0).await.unwrap();
        assert_eq!(seg.next_lsn(), 0);
        drop(seg);

        let path = dir.path().join(segment_filename(0));
        let mut reader = SegmentReader::open(&path).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(reader.last_good_offset(), HEADER_BYTES);
    }

    #[tokio::test]
    async fn append_sync_reopen_yields_records() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 0).await.unwrap();
        let r1 = Record::Begin(BeginPayload {
            txn: TwoPcTransaction::new(),
            user: "u".into(),
            database: "d".into(),
        });
        let r2 = Record::End(TxnPayload {
            txn: TwoPcTransaction::new(),
        });
        let mut buf = BytesMut::new();
        r1.encode(&mut buf).unwrap();
        r2.encode(&mut buf).unwrap();
        let start = seg.append_batch(&buf, 2).await.unwrap();
        seg.sync().await.unwrap();
        assert_eq!(start, 0);
        assert_eq!(seg.next_lsn(), 2);
        drop(seg);

        let path = dir.path().join(segment_filename(0));
        let mut reader = SegmentReader::open(&path).await.unwrap();
        let mut records = Vec::new();
        while let Some(r) = reader.next().await.unwrap() {
            records.push(r);
        }
        assert_eq!(records, vec![r1, r2]);
        assert_eq!(reader.next_lsn(), 2);
    }

    #[tokio::test]
    async fn torn_tail_is_truncated_via_into_writable() {
        let dir = TempDir::new().unwrap();
        let path = {
            let mut seg = Segment::create(dir.path(), 0).await.unwrap();
            let mut buf = BytesMut::new();
            Record::Begin(BeginPayload {
                txn: TwoPcTransaction::new(),
                user: "u".into(),
                database: "d".into(),
            })
            .encode(&mut buf)
            .unwrap();
            seg.append_batch(&buf, 1).await.unwrap();
            seg.sync().await.unwrap();
            // Append half a second record's framing to simulate a torn write.
            buf.clear();
            Record::End(TxnPayload {
                txn: TwoPcTransaction::new(),
            })
            .encode(&mut buf)
            .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut seg.file, &buf[..buf.len() / 2])
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::flush(&mut seg.file)
                .await
                .unwrap();
            seg.path().to_path_buf()
        };

        let mut reader = SegmentReader::open(&path).await.unwrap();
        let mut records = Vec::new();
        let term = loop {
            match reader.next().await {
                Ok(Some(r)) => records.push(r),
                Ok(None) => break Ok(()),
                Err(err) => break Err(err),
            }
        };
        assert_eq!(records.len(), 1);
        assert!(matches!(term, Err(Error::TornTail { .. })));
        let last_good = reader.last_good_offset();
        let seg = reader.into_writable().await.unwrap();
        assert_eq!(seg.size_bytes(), last_good);

        let meta = tokio::fs::metadata(&path).await.unwrap();
        assert_eq!(meta.len(), last_good);
    }

    #[tokio::test]
    async fn corrupt_record_stops_iteration_at_corruption() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 0).await.unwrap();
        let r1 = Record::Begin(BeginPayload {
            txn: TwoPcTransaction::new(),
            user: "u".into(),
            database: "d".into(),
        });
        let mut buf = BytesMut::new();
        r1.encode(&mut buf).unwrap();
        Record::End(TxnPayload {
            txn: TwoPcTransaction::new(),
        })
        .encode(&mut buf)
        .unwrap();
        seg.append_batch(&buf, 2).await.unwrap();
        seg.sync().await.unwrap();
        let path = seg.path().to_path_buf();
        drop(seg);

        let mut on_disk = tokio::fs::read(&path).await.unwrap();
        let last = on_disk.len() - 1;
        on_disk[last] ^= 0xff;
        tokio::fs::write(&path, &on_disk).await.unwrap();

        let mut reader = SegmentReader::open(&path).await.unwrap();
        let mut records = Vec::new();
        let term = loop {
            match reader.next().await {
                Ok(Some(r)) => records.push(r),
                Ok(None) => break Ok(()),
                Err(err) => break Err(err),
            }
        };
        assert_eq!(records, vec![r1]);
        assert!(matches!(term, Err(Error::Crc { .. })));
    }

    #[tokio::test]
    async fn bad_magic_is_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(segment_filename(0));
        tokio::fs::write(&path, vec![0u8; HEADER_BYTES as usize])
            .await
            .unwrap();
        assert!(matches!(
            SegmentReader::open(&path).await,
            Err(Error::BadSegmentHeader)
        ));
    }

    #[tokio::test]
    async fn missing_header_is_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(segment_filename(0));
        tokio::fs::write(&path, b"PG2W").await.unwrap();
        assert!(matches!(
            SegmentReader::open(&path).await,
            Err(Error::BadSegmentHeader)
        ));
    }

    #[tokio::test]
    async fn many_records_span_multiple_read_chunks() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 100).await.unwrap();
        let payload = "x".repeat(1024);
        let mut expected = Vec::new();
        let mut buf = BytesMut::new();
        for _ in 0..200 {
            let r = Record::Begin(BeginPayload {
                txn: TwoPcTransaction::new(),
                user: payload.clone(),
                database: "d".into(),
            });
            r.encode(&mut buf).unwrap();
            expected.push(r);
        }
        seg.append_batch(&buf, expected.len() as u32).await.unwrap();
        seg.sync().await.unwrap();
        let path = seg.path().to_path_buf();
        drop(seg);

        let mut reader = SegmentReader::open(&path).await.unwrap();
        let mut records = Vec::new();
        while let Some(r) = reader.next().await.unwrap() {
            records.push(r);
        }
        assert_eq!(records, expected);
        assert_eq!(reader.next_lsn(), 100 + expected.len() as u64);
    }
}
