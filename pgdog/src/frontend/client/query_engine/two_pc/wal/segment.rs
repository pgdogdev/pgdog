//! Two-phase commit WAL segment.

use crate::net::{Error, FromBytes};
use bytes::{Buf, Bytes};

use super::{super::Manager, Record, TwoPcRecord, TwoPcRecordAction};

pub(super) struct Segment {
    counter: u64,
    records: Vec<Record>,
}

impl Segment {
    /// Replay segment against the 2pc manager to
    /// restore in-memory state.
    pub(super) async fn replay(&self, manager: &Manager) -> Result<(), Error> {
        for record in &self.records {
            let record = TwoPcRecord::try_from(record.clone()).unwrap();
            match record.action {
                TwoPcRecordAction::Add => {
                    manager.transaction_state_manual(
                        record.transaction,
                        &record.info.identifier,
                        record.info.phase,
                    );
                }

                TwoPcRecordAction::Remove => {
                    manager.remove(record.transaction);
                }
            }
        }

        Ok(())
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

        Ok(Self { counter, records })
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

    use super::super::{TwoPcRecord, TwoPcRecordAction};
    use super::*;

    #[test]
    fn test_multiple_records() {
        let expected = [
            TwoPcRecord {
                transaction: TwoPcTransaction(123456),
                info: TransactionInfo {
                    phase: TwoPcPhase::Phase1,
                    identifier: Arc::new(User {
                        user: "pgdog".into(),
                        database: "prod".into(),
                    }),
                },
                action: TwoPcRecordAction::Add,
            },
            TwoPcRecord {
                transaction: TwoPcTransaction(654321),
                info: TransactionInfo {
                    phase: TwoPcPhase::Phase2,
                    identifier: Arc::new(User {
                        user: "admin".into(),
                        database: "postgres".into(),
                    }),
                },
                action: TwoPcRecordAction::Remove,
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
            .map(TwoPcRecord::try_from)
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
