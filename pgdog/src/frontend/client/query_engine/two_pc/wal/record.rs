use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use pgdog_stats::User;

use super::super::{TransactionInfo, TwoPcPhase, TwoPcTransaction};
use crate::net::{Error, FromBytes, Payload, ToBytes, c_string_buf};

/// Record format matches the [`crate::net::CopyData`] format.
///
/// | code | len |      payload        |
/// | char | i32 | bytes of length len |
///
#[derive(Debug, Clone)]
pub(super) struct Record {
    pub(super) code: char, // Type of record it is.
    pub(super) data: Bytes,
}

impl Record {
    pub(super) fn len(&self) -> i32 {
        self.data.len() as i32 + 5
    }
}

impl FromBytes for Record {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        if bytes.len() < 5 {
            return Err(Error::UnexpectedEof);
        }

        let code = bytes.get_u8() as char;
        let len = bytes.get_i32();
        if len < 4 {
            return Err(Error::UnexpectedEof);
        }

        let data_len = len as usize - 4;
        if bytes.remaining() < data_len {
            return Err(Error::UnexpectedEof);
        }

        let data = bytes.split_to(data_len);

        Ok(Self { code, data })
    }
}

impl ToBytes for Record {
    fn to_bytes(&self) -> Bytes {
        let mut payload = Payload::named(self.code);
        payload.put(self.data.clone());

        payload.freeze()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum TwoPcRecordAction {
    Add,
    Remove,
}

impl TryFrom<u8> for TwoPcRecordAction {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => TwoPcRecordAction::Add,
            1 => TwoPcRecordAction::Remove,
            _ => return Err(()),
        })
    }
}

impl From<TwoPcRecordAction> for u8 {
    fn from(value: TwoPcRecordAction) -> Self {
        match value {
            TwoPcRecordAction::Add => 0,
            TwoPcRecordAction::Remove => 1,
        }
    }
}

/// Two phase commit transaction record.
/// Contains all the information we need to replay it against
/// the manager.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TwoPcRecord {
    pub(super) transaction: TwoPcTransaction,
    pub(super) info: TransactionInfo,
    pub(super) action: TwoPcRecordAction,
}

impl From<TwoPcRecord> for Record {
    fn from(value: TwoPcRecord) -> Self {
        let mut payload = Payload::raw();
        payload.put_u64(value.transaction.0 as u64);
        payload.put_string(&value.info.identifier.user);
        payload.put_string(&value.info.identifier.database);
        payload.put_u8(u8::from(value.action));

        Self {
            code: char::from(value.info.phase),
            data: payload.freeze(),
        }
    }
}

impl TryFrom<Record> for TwoPcRecord {
    type Error = ();

    fn try_from(mut value: Record) -> Result<Self, Self::Error> {
        let phase = TwoPcPhase::try_from(value.code)?;
        let tid = value.data.get_u64() as usize;
        let user = c_string_buf(&mut value.data);
        let database = c_string_buf(&mut value.data);
        let action = TwoPcRecordAction::try_from(value.data.get_u8())?;

        Ok(TwoPcRecord {
            transaction: TwoPcTransaction(tid),
            info: TransactionInfo {
                phase,
                identifier: Arc::new(User { user, database }),
            },
            action,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let record = TwoPcRecord {
            transaction: TwoPcTransaction(123456),
            info: TransactionInfo {
                phase: TwoPcPhase::Phase1,
                identifier: Arc::new(User {
                    user: "pgdog".into(),
                    database: "prod".into(),
                }),
            },
            action: TwoPcRecordAction::Add,
        };

        let wal_record = Record::from(record.clone());
        let bytes = wal_record.to_bytes();
        let wal_record = Record::from_bytes(bytes).unwrap();
        let wal_record = TwoPcRecord::try_from(wal_record).unwrap();

        assert_eq!(wal_record, record);
    }

    #[test]
    fn test_invalid_length() {
        let bytes = Bytes::from_static(&[b'1', 0, 0, 0, 3]);

        assert!(matches!(
            Record::from_bytes(bytes),
            Err(Error::UnexpectedEof)
        ));
    }

    #[test]
    fn test_truncated_record() {
        let bytes = Bytes::from_static(&[b'1', 0, 0, 0, 5]);

        assert!(matches!(
            Record::from_bytes(bytes),
            Err(Error::UnexpectedEof)
        ));
    }
}
