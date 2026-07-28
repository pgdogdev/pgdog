//! Two-pc WAL records.

use super::super::super::Manager;
use super::*;

#[derive(Debug, Clone)]
pub enum Records {
    Add(TwoPcRecordAdd),
    Remove(TwoPcRecordRemove),
}

impl Records {
    pub(crate) fn replay(&self, manager: &Manager) {
        match self {
            Records::Add(add) => manager.transaction_state_manual(
                add.transaction,
                &add.info.identifier,
                add.info.phase,
            ),
            Records::Remove(remove) => {
                manager.remove(remove.transaction);
            }
        }
    }
}

impl TryFrom<Record> for Records {
    type Error = ();

    fn try_from(value: Record) -> Result<Self, Self::Error> {
        match value.code {
            '1' | '2' | '3' => Ok(Self::Add(TwoPcRecordAdd::try_from(value)?)),
            'r' => Ok(Self::Remove(TwoPcRecordRemove::try_from(value)?)),
            _ => Err(()),
        }
    }
}
