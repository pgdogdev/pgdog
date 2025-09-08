//! Two-phase commit handler.
use std::sync::atomic::{AtomicUsize, Ordering};

use once_cell::sync::Lazy;

use super::Error;

static COUNTER: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

/// Two-phase commit driver.
#[derive(Default, Debug, Clone)]
pub(super) struct TwoPc {
    name: Option<String>,
}

impl TwoPc {
    /// Get a unique name for the two-pc transaction.
    pub(super) fn name(&mut self) -> &str {
        if self.name.is_none() {
            let name = format!("__pgdog_2pc_{}", COUNTER.fetch_add(1, Ordering::Relaxed));
            self.name = Some(name);
        }

        self.name.as_ref().unwrap().as_str()
    }

    /// Start phase one of two-phase commit.
    ///
    /// If we crash during this phase, the transaction must be rolled back.
    pub(super) async fn phase_one(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// Start phase two of two-phase commit.
    ///
    /// If we crash during this phase, the transaction must be committed.
    pub(super) async fn phase_two(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// Finish two-pc transaction.
    ///
    /// This is just a cleanup step, to avoid unnecessary checks during crash recovery.
    pub(super) async fn done(&mut self) -> Result<(), Error> {
        self.name = None;
        Ok(())
    }
}
