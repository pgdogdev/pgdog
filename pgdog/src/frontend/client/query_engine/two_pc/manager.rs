//! Global two-phase commit transaction manager.

use arc_swap::ArcSwapOption;
use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    select,
    sync::Notify,
    time::{Instant, interval, sleep},
};
use tracing::{debug, error, info, warn};

use crate::{
    backend::{
        databases::User,
        pool::{Connection, Request},
    },
    frontend::router::{
        Route,
        parser::{Shard, ShardWithPriority},
    },
    tasks,
};

use super::{
    Error, TwoPcGuard, TwoPcPhase, TwoPcStats, TwoPcTransaction,
    wal::{Checkpointer, Recovery, TwoPcRecordAdd, TwoPcRecordRemove, WalWriter},
};

static MANAGER: Lazy<Manager> = Lazy::new(Manager::init);
static MAINTENANCE: Duration = Duration::from_millis(333);

/// Two-phase commit transaction manager.
#[derive(Debug, Clone)]
pub struct Manager {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<InnerNotify>,
    stats: Arc<TwoPcStats>,
    wal: Arc<ArcSwapOption<WalWriter>>,
}

impl Manager {
    /// Get transaction manager instance.
    pub fn get() -> Self {
        MANAGER.clone()
    }

    pub(super) fn init() -> Self {
        let manager = Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            notify: Arc::new(InnerNotify {
                notify: Notify::new(),
                offline: AtomicBool::new(false),
                done_flag: AtomicBool::new(false),
                done: Notify::new(),
            }),
            stats: Arc::new(TwoPcStats::default()),
            wal: Arc::new(ArcSwapOption::empty()),
        };

        let monitor = manager.clone();
        tasks::spawn("2pc monitor", async move {
            Self::monitor(monitor).await;
        });

        manager
    }

    /// Run recovery and enable the 2pc WAL writer.
    ///
    /// # Arguments
    ///
    /// - `wal_directory`: WAL directory.
    /// - `checkpoint_interval`: How frequently to run the checkpointer. If `None`,
    ///   the checkpointer is disabled.
    /// - `segment_size`: Maximum size of a WAL segment. Soft limit.
    ///
    pub async fn enable_wal(
        &self,
        wal_directory: &PathBuf,
        checkpoint_interval: Option<Duration>,
        segment_size: usize,
    ) -> Result<(), Error> {
        let writer = Recovery::new(wal_directory)
            .await?
            .run(self, segment_size)
            .await?;
        self.wal.store(Some(Arc::new(writer)));

        if let Some(checkpoint_interval) = checkpoint_interval {
            Checkpointer::spawn(wal_directory.clone(), self.clone(), checkpoint_interval);
        }

        Ok(())
    }

    pub(super) fn transaction(&self, transaction: &TwoPcTransaction) -> Option<TransactionInfo> {
        self.inner.lock().transactions.get(transaction).cloned()
    }

    /// Get all active two-phase transactions.
    pub fn transactions(&self) -> HashMap<TwoPcTransaction, TransactionInfo> {
        self.inner.lock().transactions.clone()
    }

    /// Process-level 2PC counters.
    pub fn stats(&self) -> Arc<TwoPcStats> {
        Arc::clone(&self.stats)
    }

    /// Two-pc transaction finished.
    pub(crate) async fn done(&self, transaction: TwoPcTransaction) -> Result<(), Error> {
        if let Some(_) = self.remove(transaction) {
            if let Some(wal) = self.wal.load_full() {
                wal.add(TwoPcRecordRemove { transaction }).await?;
            }
        }

        Ok(())
    }

    /// Block until the monitor has removed this transaction from the manager,
    /// or until a fixed timeout elapses.
    ///
    /// No-op if the transaction was never registered or is already gone.
    pub async fn wait_until_cleaned_up(&self, transaction: TwoPcTransaction) {
        const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if !self.inner.lock().transactions.contains_key(&transaction) {
                return;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                warn!(
                    "[2pc] timed out waiting for transaction {} cleanup; monitor will retry",
                    transaction
                );
                return;
            }
            select! {
                _ = self.notify.notify.notified() => {}
                _ = sleep(remaining) => {}
            }
        }
    }

    /// Record a phase transition for a 2PC transaction.
    ///
    /// # Arguments
    ///
    /// - `transaction`: 2pc transaction.
    /// - `identifer`: User/database where the transaction is being run.
    /// - `phase`: Transaction phase, e.g., phase I or phase II.
    ///
    pub(crate) async fn transaction_state(
        &self,
        transaction: TwoPcTransaction,
        identifier: &Arc<User>,
        phase: TwoPcPhase,
    ) -> Result<TwoPcGuard, Error> {
        self.set_transaction_state(transaction, identifier, phase);

        if let Some(wal) = self.wal.load_full() {
            wal.add(TwoPcRecordAdd {
                transaction,
                info: TransactionInfo {
                    phase,
                    identifier: identifier.clone(),
                },
            })
            .await?;
        }

        Ok(TwoPcGuard {
            transaction,
            manager: Self::get(),
        })
    }

    /// Set the transaction state in memory.
    ///
    /// WAL is not updated. Used during recovery
    /// and before writing the transaction to the WAL during
    /// normal operations.
    pub(super) fn set_transaction_state(
        &self,
        transaction: TwoPcTransaction,
        identifier: &Arc<User>,
        phase: TwoPcPhase,
    ) {
        self.inner
            .lock()
            .transactions
            .entry(transaction)
            .and_modify(|entry| {
                entry.phase = phase;
            })
            .or_insert(TransactionInfo {
                identifier: identifier.clone(),
                phase,
            });
    }

    /// Enqueue all transactions into the cleanup manager.
    ///
    /// This is called by recovery only.
    ///
    pub(super) fn cleanup_all(&self) {
        let mut guard = self.inner.lock();
        for transaction in guard.transactions.keys().cloned().collect::<Vec<_>>() {
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
                match manager.cleanup_phase(transaction).await {
                    Err(err) => {
                        error!(
                            r#"[2pc] error cleaning up "{}" transaction: {}"#,
                            transaction.to_string(),
                            err
                        );

                        // Retry again later.
                        manager.inner.lock().queue.push_back(transaction);
                    }
                    _ => {
                        manager.done(transaction).await.unwrap();
                    }
                }

                notify.notify.notify_one();
            } else if notify.offline.load(Ordering::Relaxed) {
                // No more transactions to cleanup.
                notify.done_flag.store(true, Ordering::Relaxed);
                notify.done.notify_waiters();
                break;
            }
        }
    }

    pub(super) fn remove(&self, transaction: TwoPcTransaction) -> Option<TransactionInfo> {
        self.inner.lock().transactions.remove(&transaction)
    }

    /// Reconnect to cluster if available and rollback the two-phase transaction.
    async fn cleanup_phase(&self, transaction: TwoPcTransaction) -> Result<(), Error> {
        let state = match self.inner.lock().transactions.get(&transaction).cloned() {
            Some(state) => state,
            _ => {
                return Ok(());
            }
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
        connection.two_pc(transaction, phase, true).await?;
        connection.disconnect();

        Ok(())
    }

    /// Shutdown manager and wait for all transactions to be cleaned up.
    /// Once the monitor has drained the cleanup queue, the WAL is shut
    /// down too so any final End records make it to disk before exit.
    pub async fn shutdown(&self) {
        if self.notify.done_flag.load(Ordering::Relaxed) {
            return;
        }

        let waiter = self.notify.done.notified();
        self.notify.offline.store(true, Ordering::Relaxed);
        self.notify.notify.notify_one();
        let transactions = self.inner.lock().queue.len();

        info!("cleaning up {} two-phase transactions", transactions);

        waiter.await;
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
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
    done_flag: AtomicBool,
    done: Notify,
}
