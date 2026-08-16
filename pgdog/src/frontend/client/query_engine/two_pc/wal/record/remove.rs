use super::super::super::TwoPcTransaction;
use super::*;

/// Record indicating the transaction was safely
/// removed from the 2pc manager.
#[derive(Debug, Clone)]
pub(crate) struct TwoPcRecordRemove {
    pub(crate) transaction: TwoPcTransaction,
}

impl TryFrom<Record> for TwoPcRecordRemove {
    type Error = ();

    fn try_from(mut value: Record) -> Result<Self, Self::Error> {
        let tid = value.data.get_u64() as usize;

        Ok(TwoPcRecordRemove {
            transaction: TwoPcTransaction::from_id(tid),
        })
    }
}

impl From<TwoPcRecordRemove> for Record {
    fn from(value: TwoPcRecordRemove) -> Self {
        let mut payload = Payload::raw();
        payload.put_u64(value.transaction.id() as u64);

        Record {
            code: 'r',
            data: payload.freeze(),
        }
    }
}
