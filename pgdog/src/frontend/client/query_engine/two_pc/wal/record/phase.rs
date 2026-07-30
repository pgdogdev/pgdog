use bytes::{Buf, BufMut};

use super::super::super::{TwoPcPhase, TwoPcTransaction};
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
    pub(crate) phase: TwoPcPhase,
}

impl TwoPcRecordPhase {
    pub(crate) fn new(transaction: TwoPcTransaction, phase: TwoPcPhase) -> Self {
        Self { transaction, phase }
    }
}

impl From<TwoPcRecordPhase> for Record {
    fn from(value: TwoPcRecordPhase) -> Self {
        let mut payload = Payload::raw();
        payload.put_u64(value.transaction.0 as u64);

        Self {
            code: char::from(value.phase),
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

        let phase = TwoPcPhase::try_from(value.code)?;
        let transaction = TwoPcTransaction(value.data.get_u64() as usize);

        Ok(Self { transaction, phase })
    }
}
