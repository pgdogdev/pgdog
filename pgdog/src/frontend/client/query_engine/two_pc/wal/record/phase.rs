use bytes::{Buf, BufMut};

use super::super::super::TwoPcTransaction;
use super::Record;
use crate::net::Payload;

/// Two-phase commit transaction state transition.
///
/// # Format
///
/// | Column | Data type | Length |
/// |--------|-----------|--------|
/// | tid    | u64       | 8      |
///
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TwoPcRecordPhase {
    pub(crate) transaction: TwoPcTransaction,
}

impl TwoPcRecordPhase {
    pub(crate) fn new(transaction: TwoPcTransaction) -> Self {
        Self { transaction }
    }
}

impl From<TwoPcRecordPhase> for Record {
    fn from(value: TwoPcRecordPhase) -> Self {
        let mut payload = Payload::raw();
        payload.put_u64(value.transaction.id() as u64);

        Self {
            code: '2',
            data: payload.freeze(),
        }
    }
}

impl TryFrom<Record> for TwoPcRecordPhase {
    type Error = ();

    fn try_from(mut value: Record) -> Result<Self, Self::Error> {
        if value.data.remaining() != size_of::<u64>() {
            return Err(());
        }

        if value.code != '2' {
            return Err(());
        }
        let transaction = TwoPcTransaction::from_id(value.data.get_u64() as usize);

        Ok(Self { transaction })
    }
}
