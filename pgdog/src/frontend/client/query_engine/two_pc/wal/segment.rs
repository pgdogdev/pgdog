//! Two-phase commit WAL segment.

use std::path::{Path, PathBuf};

use crate::net::Error;
use bytes::Bytes;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};
use tracing::debug;

use super::{super::Manager, Record, Records};

#[derive(Debug, Clone)]
pub(crate) struct Segment {
    pub(super) segment_id: u64,
    // Keeping the version around. Allows us to switch WAL format
    // versions later.
    #[allow(dead_code)]
    version: u32,
    pub(super) records: Vec<Record>,
}

impl Segment {
    /// Replay segment against the 2pc manager to
    /// restore in-memory state.
    pub(super) fn replay(&self, manager: &Manager) -> Result<(), Error> {
        for record in &self.records {
            let record = Records::try_from(record.clone()).expect("invalid record");
            record.replay(manager);
        }

        Ok(())
    }

    /// Load segment from file.
    pub(super) async fn load(path: &Path) -> Result<Self, Error> {
        use std::io::ErrorKind;
        debug!(r#"[2pc] opening segment "{}""#, path.display());

        let segment = File::open(path).await?;
        let file_len = segment.metadata().await?.len() as usize;
        let mut buffer = BufReader::new(segment);

        let counter = buffer.read_u64().await?;
        let version = buffer.read_u32().await?;
        let mut records = vec![];
        let mut consumed = size_of::<u64>() + size_of::<u32>();

        // This can safely read incomplete segments,
        // which is expected if PgDog crashed during a 2pc write.
        loop {
            let code = match buffer.read_u8().await {
                Ok(code) => code,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            };
            consumed += size_of::<u8>();

            let len = match buffer.read_i32().await {
                Ok(len) => len,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            };
            consumed += size_of::<i32>();

            if len < 4 {
                return Err(Error::MalformedMessageLength(len));
            }

            let data_len = len as usize - size_of::<i32>();
            if data_len > file_len.saturating_sub(consumed) {
                break;
            }

            let mut data = vec![0u8; data_len];
            match buffer.read_exact(&mut data).await {
                Ok(_) => (),
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err.into()),
            }

            consumed += data_len;

            records.push(Record {
                code: code as char,
                data: Bytes::from(data),
            });
        }

        Ok(Self {
            segment_id: counter,
            version,
            records,
        })
    }

    pub(crate) fn size(&self) -> usize {
        self.records.iter().map(|record| record.len()).sum()
    }

    // Path to the segment.
    pub(super) fn path(path: &Path, number: u64) -> PathBuf {
        use super::EXTENSION;

        path.join(format!("{}.{}", number, EXTENSION))
    }

    #[cfg(test)]
    pub(super) fn records(&self) -> &[Record] {
        &self.records
    }
}
