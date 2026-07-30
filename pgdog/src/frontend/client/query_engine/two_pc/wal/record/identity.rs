use std::sync::Arc;

use bytes::{Buf, BufMut};
use pgdog_stats::User;

use super::super::super::TwoPcTransaction;
use super::Record;
use crate::net::{Payload, c_string_buf};

/// Identity of a new two-phase commit transaction.
///
/// This record also establishes Phase 1 during recovery, so the identity is
/// written only once per transaction.
///
/// # Format
///
/// | Column   | Data type | Length   |
/// |----------|-----------|----------|
/// | tid      | u64       | 8        |
/// | user     | string    | variable |
/// | database | string    | variable |
///
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TwoPcRecordIdentity {
    pub(crate) transaction: TwoPcTransaction,
    pub(crate) identifier: Arc<User>,
}

impl From<TwoPcRecordIdentity> for Record {
    fn from(value: TwoPcRecordIdentity) -> Self {
        let mut payload = Payload::raw();
        payload.put_u64(value.transaction.0 as u64);
        payload.put_string(&value.identifier.user);
        payload.put_string(&value.identifier.database);

        Record {
            code: 'i',
            data: payload.freeze(),
        }
    }
}

impl TryFrom<Record> for TwoPcRecordIdentity {
    type Error = ();

    fn try_from(mut value: Record) -> Result<Self, Self::Error> {
        let transaction = TwoPcTransaction(value.data.get_u64() as usize);
        let user = c_string_buf(&mut value.data);
        let database = c_string_buf(&mut value.data);

        Ok(Self {
            transaction,
            identifier: Arc::new(User { user, database }),
        })
    }
}
