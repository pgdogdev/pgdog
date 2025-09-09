//! Two-phase commit handler.
use std::sync::Arc;

use crate::backend::databases::User;

use super::Error;

pub mod guard;
pub mod manager;
pub mod phase;
pub mod transaction;

pub use guard::TwoPcGuard;
pub use manager::Manager;
pub use phase::TwoPcPhase;
pub use transaction::TwoPcTransaction;

#[cfg(test)]
mod test;

/// Two-phase commit driver.
#[derive(Debug, Clone)]
pub(super) struct TwoPc {
    transaction: Option<TwoPcTransaction>,
    manager: Manager,
    auto: bool,
}

impl Default for TwoPc {
    fn default() -> Self {
        Self {
            transaction: None,
            manager: Manager::get(),
            auto: false,
        }
    }
}

impl TwoPc {
    /// Get a unique name for the two-pc transaction.
    pub(super) fn transaction(&mut self) -> TwoPcTransaction {
        if self.transaction.is_none() {
            self.transaction = Some(TwoPcTransaction::new());
        }

        self.transaction.unwrap()
    }

    /// Start phase one of two-phase commit.
    ///
    /// If we crash during this phase, the transaction must be rolled back.
    pub(super) async fn phase_one(&mut self, cluster: &Arc<User>) -> Result<TwoPcGuard, Error> {
        let transaction = self.transaction();
        self.manager
            .transaction_state(&transaction, cluster, TwoPcPhase::Phase1)
            .await
    }

    /// Start phase two of two-phase commit.
    ///
    /// If we crash during this phase, the transaction must be committed.
    pub(super) async fn phase_two(&mut self, cluster: &Arc<User>) -> Result<TwoPcGuard, Error> {
        let transaction = self.transaction();
        self.manager
            .transaction_state(&transaction, cluster, TwoPcPhase::Phase2)
            .await
    }

    /// Finish two-pc transaction.
    ///
    /// This is just a cleanup step, to avoid unnecessary checks during crash recovery.
    pub(super) async fn done(&mut self) -> Result<(), Error> {
        let transaction = self.transaction();
        self.manager.done(&transaction).await?;
        self.transaction = None;
        self.auto = false;
        Ok(())
    }

    pub fn set_auto(&mut self) {
        self.auto = true;
    }

    pub fn auto(&self) -> bool {
        self.auto
    }
}
