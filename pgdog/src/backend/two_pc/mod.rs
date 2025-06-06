//! Two-phase commit.

use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::backend::pool::Connection;

mod error;
use error::Error;

static COUNTER: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

fn transaction_name(counter: usize) -> String {
    format!("__pgdog_2pc_{}", counter)
}

pub struct TwoPc {
    counter: Option<usize>,
}

impl TwoPc {
    /// Start two-phase transaction.
    pub async fn start(&mut self) -> Result<(), Error> {
        if self.counter.is_some() {
            return Err(Error::AlreadyStarted);
        }
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        self.counter = Some(counter);

        Ok(())
    }

    /// Prepare transaction for commit.
    pub async fn prepare(&mut self, conn: &mut Connection) -> Result<(), Error> {
        if let Some(counter) = self.counter {
            conn.execute(&format!(
                "PREPARE TRANSACTION '{}'",
                transaction_name(counter)
            ))
            .await?;
        }

        Ok(())
    }

    /// Commit prepared transaction.
    pub async fn commit(&mut self, conn: &mut Connection) -> Result<(), Error> {
        if let Some(counter) = self.counter.take() {
            conn.execute(&format!("COMMIT PREPARED '{}'", transaction_name(counter)))
                .await?;
        }
        Ok(())
    }

    /// Rollback prepared transaction.
    pub async fn rollback(&mut self, conn: &mut Connection) -> Result<(), Error> {
        if let Some(counter) = self.counter.take() {
            conn.sync().await?;
            conn.execute(&format!(
                "ROLLBACK PREPARED '{}'",
                transaction_name(counter)
            ))
            .await?;
        }
        Ok(())
    }
}
