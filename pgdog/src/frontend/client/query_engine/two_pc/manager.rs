use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{select, spawn, sync::Notify, time::interval};
use tracing::{debug, error};

use crate::{
    backend::{
        databases::User,
        pool::{Connection, Request},
    },
    frontend::{
        client::query_engine::{
            two_pc::{TwoPcGuard, TwoPcTransaction},
            TwoPcPhase,
        },
        router::{parser::Shard, Route},
    },
};

use super::Error;

static MANAGER: Lazy<Manager> = Lazy::new(Manager::init);

#[derive(Debug, Clone)]
pub struct Manager {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
}

impl Manager {
    /// Get transaction manager instance.
    pub(crate) fn get() -> Self {
        MANAGER.clone()
    }

    fn init() -> Self {
        let manager = Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            notify: Arc::new(Notify::new()),
        };

        let monitor = manager.clone();
        spawn(async move {
            Self::monitor(monitor).await;
        });

        manager
    }

    /// Two-pc transaction finished.
    pub(super) async fn done(&self, transaction: &TwoPcTransaction) -> Result<(), Error> {
        self.remove(transaction).await;

        Ok(())
    }

    /// Sync transaction state.
    pub(super) async fn transaction_state(
        &self,
        transaction: &TwoPcTransaction,
        identifier: &Arc<User>,
        phase: TwoPcPhase,
    ) -> Result<TwoPcGuard, Error> {
        let mut guard = self.inner.lock();
        let entry = guard
            .transactions
            .entry(transaction.clone())
            .or_insert_with(TransactionInfo::default);
        entry.identifier = identifier.clone();
        entry.phase = phase;

        // TODO: Sync to durable backend.

        Ok(TwoPcGuard {
            phase,
            transaction: transaction.clone(),
            manager: Self::get(),
            identifier: identifier.clone(),
        })
    }

    pub(super) fn return_guard(&self, guard: &TwoPcGuard) {
        let exists = self
            .inner
            .lock()
            .transactions
            .contains_key(&guard.transaction);

        if exists {
            self.inner.lock().queue.push_back(guard.transaction);
        }
    }

    async fn monitor(manager: Self) {
        let mut interval = interval(Duration::from_millis(333));
        let notify = manager.notify.clone();

        debug!("[2pc] monitor started");

        loop {
            // Wake up either because it's time to check
            // or manager told us to.
            select! {
                _ = interval.tick() => (),
                _ = notify.notified() => (),
            }

            let transaction = manager.inner.lock().queue.pop_front();
            if let Some(transaction) = transaction {
                if let Err(err) = manager.cleanup_phase(&transaction).await {
                    error!("[2pc] error: {}", err);

                    // Retry again later.
                    manager.inner.lock().queue.push_back(transaction);
                } else {
                    manager.remove(&transaction).await;
                }

                notify.notify_one();
            }
        }
    }

    async fn remove(&self, transaction: &TwoPcTransaction) {
        self.inner.lock().transactions.remove(transaction);
        // TODO: sync to durable stage manager here.
    }

    /// Reconnect to cluster if available and rollback the two-phase transaction.
    async fn cleanup_phase(&self, transaction: &TwoPcTransaction) -> Result<(), Error> {
        let state = if let Some(state) = self.inner.lock().transactions.get(transaction).cloned() {
            state
        } else {
            return Ok(());
        };

        let phase = match state.phase {
            TwoPcPhase::Phase1 => TwoPcPhase::Rollback,
            phase => phase,
        };

        let mut connection = Connection::new(
            &state.identifier.user,
            &state.identifier.database,
            false,
            &None,
        )?;

        connection
            .connect(&Request::default(), &Route::write(Shard::All))
            .await?;
        connection.two_pc(&transaction.to_string(), phase).await?;
        connection.disconnect();

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct TransactionInfo {
    phase: TwoPcPhase,
    identifier: Arc<User>,
}

#[derive(Default, Debug)]
struct Inner {
    transactions: HashMap<TwoPcTransaction, TransactionInfo>,
    queue: VecDeque<TwoPcTransaction>,
}
