//! Global two-phase commit transaction manager.
use arc_swap::ArcSwapOption;
use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{select, spawn, sync::Notify, time::interval};
use tracing::{debug, error, info, warn};

use crate::{
    backend::{
        databases::User,
        pool::{Connection, Request},
    },
    frontend::{
        client::query_engine::{
            two_pc::{wal::Wal, TwoPcGuard, TwoPcTransaction},
            TwoPcPhase,
        },
        router::{
            parser::{Shard, ShardWithPriority},
            Route,
        },
    },
};

use super::Error;

static MANAGER: Lazy<Manager> = Lazy::new(Manager::init);
static MAINTENANCE: Duration = Duration::from_millis(333);

/// Two-phase commit transaction manager.
#[derive(Debug, Clone)]
pub struct Manager {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<InnerNotify>,
    /// Durable log handle. `None` until [`Self::start`] succeeds; if WAL
    /// initialization fails or `start` is never called, the manager
    /// continues to coordinate 2PC in memory only.
    wal: Arc<ArcSwapOption<Wal>>,
}

impl Manager {
    /// Get transaction manager instance.
    pub fn get() -> Self {
        MANAGER.clone()
    }

    fn init() -> Self {
        let manager = Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            notify: Arc::new(InnerNotify {
                notify: Notify::new(),
                offline: AtomicBool::new(false),
                done: Notify::new(),
            }),
            wal: Arc::new(ArcSwapOption::const_empty()),
        };

        let monitor = manager.clone();
        spawn(async move {
            Self::monitor(monitor).await;
        });

        manager
    }

    /// Open the WAL at the configured directory, replay any in-flight
    /// transactions back into this manager, and start the writer task.
    /// If probing or recovery fail, the manager keeps running without
    /// durability and a warning is logged so operators can investigate.
    pub async fn start(&self) {
        match Wal::open(self).await {
            Ok(wal) => {
                self.wal.store(Some(Arc::new(wal)));
                info!("[2pc] wal enabled");
            }
            Err(err) => {
                warn!(
                    "[2pc] wal disabled: {}; 2pc will run without durability",
                    err
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn transaction(&self, transaction: &TwoPcTransaction) -> Option<TransactionInfo> {
        self.inner.lock().transactions.get(transaction).cloned()
    }

    /// Get all active two-phase transactions.
    pub fn transactions(&self) -> HashMap<TwoPcTransaction, TransactionInfo> {
        self.inner.lock().transactions.clone()
    }

    /// Two-pc transaction finished.
    pub(super) async fn done(&self, transaction: &TwoPcTransaction) -> Result<(), Error> {
        self.remove(transaction).await;

        Ok(())
    }

    /// Record a phase transition for a 2PC transaction. Writes the
    /// corresponding WAL record (Begin for Phase1, Committing for
    /// Phase2) before mutating in-memory state so the on-disk log
    /// always leads the coordinator. WAL write failures are logged but
    /// do not block the 2PC operation — durability is best-effort.
    pub(super) async fn transaction_state(
        &self,
        transaction: &TwoPcTransaction,
        identifier: &Arc<User>,
        phase: TwoPcPhase,
    ) -> Result<TwoPcGuard, Error> {
        if let Some(wal) = self.wal.load_full() {
            let result = match phase {
                TwoPcPhase::Phase1 => {
                    wal.append_begin(
                        *transaction,
                        identifier.user.clone(),
                        identifier.database.clone(),
                    )
                    .await
                }
                TwoPcPhase::Phase2 => wal.append_committing(*transaction).await,
                TwoPcPhase::Rollback => {
                    unreachable!("rollback is not a state transition; it's the cleanup direction")
                }
            };
            if let Err(err) = result {
                warn!(
                    "[2pc] wal append failed for {} ({}): {}; transaction proceeds without durability",
                    transaction, phase, err
                );
            }
        }

        {
            let mut guard = self.inner.lock();
            let entry = guard.transactions.entry(*transaction).or_default();
            entry.identifier = identifier.clone();
            entry.phase = phase;
        }

        Ok(TwoPcGuard {
            transaction: *transaction,
            manager: Self::get(),
        })
    }

    /// Restore an in-flight 2PC transaction discovered during WAL
    /// recovery. Inserts it into the transaction table and pushes it onto
    /// the cleanup queue so the monitor task drives it to a terminal
    /// state via [`Self::cleanup_phase`].
    pub(super) fn restore_transaction(
        &self,
        transaction: TwoPcTransaction,
        user: String,
        database: String,
        phase: TwoPcPhase,
    ) {
        let identifier = Arc::new(User { user, database });
        {
            let mut guard = self.inner.lock();
            guard
                .transactions
                .insert(transaction, TransactionInfo { phase, identifier });
            guard.queue.push_back(transaction);
        }
        self.notify.notify.notify_one();
    }

    pub(super) fn return_guard(&self, guard: &TwoPcGuard) {
        let exists = self
            .inner
            .lock()
            .transactions
            .contains_key(&guard.transaction);

        if exists {
            self.inner.lock().queue.push_back(guard.transaction);
            self.notify.notify.notify_one();
        }
    }

    async fn monitor(manager: Self) {
        let mut interval = interval(MAINTENANCE);
        let notify = manager.notify.clone();

        debug!("[2pc] monitor started");

        loop {
            // Wake up either because it's time to check
            // or manager told us to.
            select! {
                _ = interval.tick() => (),
                _ = notify.notify.notified() => (),
            }

            let transaction = manager.inner.lock().queue.pop_front();
            if let Some(transaction) = transaction {
                debug!(
                    r#"[2pc] cleaning up transaction "{}""#,
                    transaction.to_string()
                );
                if let Err(err) = manager.cleanup_phase(&transaction).await {
                    error!(
                        r#"[2pc] error cleaning up "{}" transaction: {}"#,
                        transaction.to_string(),
                        err
                    );

                    // Retry again later.
                    manager.inner.lock().queue.push_back(transaction);
                } else {
                    manager.remove(&transaction).await;
                }

                notify.notify.notify_one();
            } else if notify.offline.load(Ordering::Relaxed) {
                // No more transactions to cleanup.
                notify.done.notify_waiters();
                break;
            }
        }
    }

    async fn remove(&self, transaction: &TwoPcTransaction) {
        if let Some(wal) = self.wal.load_full() {
            if let Err(err) = wal.append_end(*transaction).await {
                warn!("[2pc] wal end record failed for {}: {}", transaction, err);
            }
        }
        self.inner.lock().transactions.remove(transaction);
    }

    /// Reconnect to cluster if available and rollback the two-phase transaction.
    async fn cleanup_phase(&self, transaction: &TwoPcTransaction) -> Result<(), Error> {
        let state = if let Some(state) = self.inner.lock().transactions.get(transaction).cloned() {
            state
        } else {
            return Ok(());
        };

        let phase = match state.phase {
            // Phase 1 gets rolled back.
            TwoPcPhase::Phase1 => TwoPcPhase::Rollback,
            // Phase 2 gets committed.
            phase => phase,
        };

        let mut connection =
            match Connection::new(&state.identifier.user, &state.identifier.database, false) {
                Ok(conn) => conn,
                Err(err) => {
                    // Database got removed from config.
                    if matches!(err, crate::backend::Error::NoDatabase(_)) {
                        return Ok(());
                    } else {
                        return Err(err.into());
                    }
                }
            };

        connection
            .connect(
                &Request::default(),
                &Route::write(ShardWithPriority::new_override_transaction(Shard::All)),
            )
            .await?;
        connection.two_pc(&transaction.to_string(), phase).await?;
        connection.disconnect();

        Ok(())
    }

    /// Shutdown manager and wait for all transactions to be cleaned up.
    /// Once the monitor has drained the cleanup queue, the WAL is shut
    /// down too so any final End records make it to disk before exit.
    pub async fn shutdown(&self) {
        let waiter = self.notify.done.notified();
        self.notify.offline.store(true, Ordering::Relaxed);
        let transactions = self.inner.lock().queue.len();

        info!("cleaning up {} two-phase transactions", transactions);

        waiter.await;

        if let Some(wal) = self.wal.load_full() {
            wal.shutdown().await;
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct TransactionInfo {
    pub phase: TwoPcPhase,
    pub identifier: Arc<User>,
}

#[derive(Default, Debug)]
struct Inner {
    transactions: HashMap<TwoPcTransaction, TransactionInfo>,
    queue: VecDeque<TwoPcTransaction>,
}

#[derive(Debug)]
struct InnerNotify {
    notify: Notify,
    offline: AtomicBool,
    done: Notify,
}
