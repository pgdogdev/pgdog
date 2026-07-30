//! Two-pc WAL records enum.
//!
//! If adding records, add it here too and
//! tell us how to handle it during recovery.
//! Thank you!
//!

use super::super::super::{Manager, TwoPcPhase};
use super::*;

#[derive(Debug, Clone)]
pub(crate) enum Records {
    Identity(TwoPcRecordIdentity),
    Phase(TwoPcRecordPhase),
    Remove(TwoPcRecordRemove),
}

impl Records {
    /// Replay record against the two phase commit
    /// transaction manager.
    pub(crate) fn replay(&self, manager: &Manager) {
        match self {
            Records::Identity(identity) => {
                manager.set_transaction_identity(identity.transaction, &identity.identifier);
            }
            Records::Phase(phase) => {
                manager.set_transaction_phase(phase.transaction, TwoPcPhase::Phase2);
            }
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
            'i' => Ok(Self::Identity(TwoPcRecordIdentity::try_from(value)?)),
            '2' => Ok(Self::Phase(TwoPcRecordPhase::try_from(value)?)),
            'r' => Ok(Self::Remove(TwoPcRecordRemove::try_from(value)?)),
            _ => Err(()),
        }
    }
}
