//! Two-phase commit WAL errors.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("encode: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    #[error("decode: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    #[error("crc mismatch: expected {expected:#010x}, got {actual:#010x}")]
    Crc { expected: u32, actual: u32 },

    #[error("invalid record tag {0}")]
    InvalidTag(u8),

    #[error("record body length is zero")]
    EmptyRecord,

    #[error("record of {0} bytes exceeds u32 framing")]
    RecordTooLarge(usize),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("segment header is missing or has wrong magic")]
    BadSegmentHeader,

    #[error("segment filename is not a valid LSN: {0}")]
    BadSegmentName(String),

    #[error("writer task is no longer running")]
    WriterGone,

    #[error("wal directory {dir} is not accessible: {source}")]
    DirNotAccessible {
        dir: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("wal directory {dir} is not readable: {source}")]
    DirNotReadable {
        dir: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("wal directory {dir} is not writable: {source}")]
    DirNotWritable {
        dir: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}
