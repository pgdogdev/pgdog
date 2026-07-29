use std::sync::Arc;

use bytes::{Buf, BufMut};
use pgdog_stats::User;

use super::super::super::{TransactionInfo, TwoPcPhase, TwoPcTransaction};
use super::Record;

use crate::net::{Payload, c_string_buf};

/// Two phase commit transaction record.
/// Contains all the information we need to replay it against
/// the manager.
///
/// # Format
///
/// | Column   | Data type | Length   |
/// |----------|-----------|----------|
/// | tid      | u64       | 8        |
/// | user     | string    | variable |
/// | database | string    | variable |
///
///
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TwoPcRecordAdd {
    /// Transaction identifier.
    pub(crate) transaction: TwoPcTransaction,
    /// Transaction data (2pc phase and user/database).
    pub(crate) info: TransactionInfo,
}

impl From<TwoPcRecordAdd> for Record {
    fn from(value: TwoPcRecordAdd) -> Self {
        let mut payload = Payload::raw();
        payload.put_u64(value.transaction.0 as u64);
        payload.put_string(&value.info.identifier.user);
        payload.put_string(&value.info.identifier.database);

        Self {
            code: char::from(value.info.phase),
            data: payload.freeze(),
        }
    }
}

impl TryFrom<Record> for TwoPcRecordAdd {
    type Error = ();

    fn try_from(mut value: Record) -> Result<Self, Self::Error> {
        let phase = TwoPcPhase::try_from(value.code)?;
        let tid = value.data.get_u64() as usize;
        let user = c_string_buf(&mut value.data);
        let database = c_string_buf(&mut value.data);

        Ok(TwoPcRecordAdd {
            transaction: TwoPcTransaction(tid),
            info: TransactionInfo {
                phase,
                identifier: Arc::new(User { user, database }),
            },
        })
    }
}
