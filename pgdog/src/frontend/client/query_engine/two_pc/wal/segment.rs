//! Two-phase commit WAL segment.

use std::path::PathBuf;

use crate::net::{Error, FromBytes};
use bytes::{Buf, Bytes};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader, ReadBuf},
};

use super::{
    super::Manager,
    Record,
    record::{Records, TwoPcRecordAction, TwoPcRecordAdd, TwoPcRecordRemove},
};

pub(crate) struct Segment {
    pub(super) counter: u64,
    version: u32,
    records: Vec<Record>,
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
    pub(super) async fn load(path: &PathBuf) -> Result<Self, Error> {
        let segment = File::open(path).await?;
        let mut buffer = BufReader::new(segment);

        let counter = buffer.read_u64().await?;
        let version = buffer.read_u32().await?;
        let mut records = vec![];

        // This can safely read incomplete segments,
        // which is expected if PgDog crashed during a 2pc write.
        loop {
            if let Ok(code) = buffer.read_u8().await {
                if let Ok(len) = buffer.read_u64().await {
                    let mut data = vec![0u8; len as usize - 4];
                    if let Ok(_) = buffer.read_exact(&mut data).await {
                        records.push(Record {
                            code: code as char,
                            data: Bytes::from(data),
                        });
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(Self {
            counter,
            version,
            records,
        })
    }

    pub(crate) fn size(&self) -> usize {
        self.records.iter().map(|record| record.len()).sum()
    }
}

impl FromBytes for Segment {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let mut records = vec![];

        if bytes.remaining() < 8 {
            return Err(Error::UnexpectedEof);
        }

        // Finally, 64-bit xids :)
        // That's a joke, this is not an XID, this is just
        // the segment number.
        let counter = bytes.get_u64();

        if bytes.remaining() < 4 {
            return Err(Error::UnexpectedEof);
        }

        let version = bytes.get_u32();

        while bytes.has_remaining() {
            if bytes.remaining() < 5 {
                return Err(Error::UnexpectedEof);
            }

            let mut header = bytes.clone();
            header.advance(1);
            let len = header.get_i32();
            if len < 4 {
                return Err(Error::UnexpectedEof);
            }

            let record_len = len as usize + 1;
            if bytes.remaining() < record_len {
                return Err(Error::UnexpectedEof);
            }

            records.push(Record::from_bytes(bytes.split_to(record_len))?);
        }

        Ok(Self {
            counter,
            records,
            version,
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use bytes::{BufMut, BytesMut};
    use pgdog_stats::User;

    use crate::frontend::client::query_engine::two_pc::{
        TransactionInfo, TwoPcPhase, TwoPcTransaction,
    };
    use crate::net::ToBytes;

    use super::*;

    #[test]
    fn test_multiple_records() {
        let expected = [
            TwoPcRecordAdd {
                transaction: TwoPcTransaction(123456),
                info: TransactionInfo {
                    phase: TwoPcPhase::Phase1,
                    identifier: Arc::new(User {
                        user: "pgdog".into(),
                        database: "prod".into(),
                    }),
                },
            },
            TwoPcRecordAdd {
                transaction: TwoPcTransaction(654321),
                info: TransactionInfo {
                    phase: TwoPcPhase::Phase2,
                    identifier: Arc::new(User {
                        user: "admin".into(),
                        database: "postgres".into(),
                    }),
                },
            },
        ];
        let mut bytes = BytesMut::new();
        bytes.put_u64(42);
        for record in &expected {
            bytes.put(Record::from(record.clone()).to_bytes());
        }

        let segment = Segment::from_bytes(bytes.freeze()).unwrap();
        assert_eq!(segment.counter, 42);
        let records = segment
            .records
            .into_iter()
            .map(TwoPcRecordAdd::try_from)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(records, expected);
    }

    #[test]
    fn test_empty_segment() {
        let mut bytes = BytesMut::new();
        bytes.put_u64(42);

        let segment = Segment::from_bytes(bytes.freeze()).unwrap();

        assert_eq!(segment.counter, 42);
        assert!(segment.records.is_empty());
    }

    #[test]
    fn test_invalid_record_length() {
        let mut bytes = BytesMut::new();
        bytes.put_u64(42);
        bytes.put_slice(&[b'1', 0, 0, 0, 3]);

        assert!(matches!(
            Segment::from_bytes(bytes.freeze()),
            Err(Error::UnexpectedEof)
        ));
    }

    #[test]
    fn test_truncated_record() {
        let mut bytes = BytesMut::new();
        bytes.put_u64(42);
        bytes.put_slice(&[b'1', 0, 0, 0, 5]);

        assert!(matches!(
            Segment::from_bytes(bytes.freeze()),
            Err(Error::UnexpectedEof)
        ));
    }
}
