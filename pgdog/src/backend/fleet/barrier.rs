//! Pause writes to omnisharded tables for a database while its
//! topology changes underneath them.
//!
//! Used by `ADD SHARD`: omni writes must reach every shard, so they
//! pause for the brief window between draining replication to the new
//! shard and swapping it into the topology. Everything else — sharded
//! traffic, all reads — flows normally.
//!
//! Like maintenance mode, this is independent from the config and
//! holds across config reloads.
//!
use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::Arc,
};

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tracing::warn;

static OMNI_WRITE_BARRIER: Lazy<OmniWriteBarrier> = Lazy::new(|| OmniWriteBarrier {
    state: ArcSwap::from_pointee(BarrierState::default()),
    write_lock: Mutex::new(()),
});

pub(crate) fn waiter(database: &str) -> Option<Waiter> {
    OMNI_WRITE_BARRIER.get_waiter(database)
}

/// Future that resolves once omnisharded writes resume for a database.
///
/// Wraps the broadcast receiver so callers can simply `.await` it; it
/// resolves when the channel is closed (the sender is dropped by `stop`).
pub(crate) struct Waiter {
    receiver: broadcast::Receiver<()>,
    database: String,
}

impl IntoFuture for Waiter {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // Resolves when the channel is closed (sender dropped).
            let _ = self.receiver.recv().await;

            // Re-check in case the barrier was re-armed between the
            // release and this task waking up.
            if let Some(waiter) = OMNI_WRITE_BARRIER.get_waiter(&self.database) {
                let _ = waiter.await;
            }
        })
    }
}

pub fn start(database: &str) {
    OMNI_WRITE_BARRIER.add(database);
    warn!(
        "writes to omnisharded tables are paused for database \"{}\"",
        database
    );
}

pub fn stop(database: &str) {
    OMNI_WRITE_BARRIER.remove(database);
    warn!(
        "writes to omnisharded tables resumed for database \"{}\"",
        database
    );
}

/// Whether omnisharded writes are currently paused for a database.
pub fn is_on(database: &str) -> bool {
    OMNI_WRITE_BARRIER.get_waiter(database).is_some()
}

#[derive(Debug)]
struct OmniWriteBarrier {
    state: ArcSwap<BarrierState>,
    write_lock: Mutex<()>,
}

#[derive(Clone, Debug, Default)]
struct BarrierState {
    // Databases whose omnisharded writes are paused.
    databases: HashMap<String, broadcast::Sender<()>>,
}

impl OmniWriteBarrier {
    /// Get a [`Waiter`] that resolves once omnisharded writes resume
    /// for the database, or `None` if they aren't paused right now.
    fn get_waiter(&self, database: &str) -> Option<Waiter> {
        let state = self.state.load();

        if state.databases.is_empty() {
            return None;
        }

        state.databases.get(database).map(|sender| Waiter {
            receiver: sender.subscribe(),
            database: database.to_string(),
        })
    }

    /// Pause omnisharded writes for a database.
    fn add(&self, database: &str) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = BarrierState::clone(&state);

        // Keep the existing channel if already paused, so current
        // waiters stay valid.
        next.databases
            .entry(database.to_string())
            .or_insert_with(|| broadcast::channel(1).0);

        self.state.store(Arc::new(next));
    }

    /// Resume omnisharded writes for a database and wake its waiters
    /// by dropping (closing) the channel.
    fn remove(&self, database: &str) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = BarrierState::clone(&state);

        next.databases.remove(database);

        self.state.store(Arc::new(next));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_no_barrier_no_waiter() {
        assert!(waiter("omni_test_db_a").is_none());
    }

    #[tokio::test]
    async fn test_waiter_resolves_on_stop() {
        start("omni_test_db_b");
        assert!(is_on("omni_test_db_b"));

        let waiter = waiter("omni_test_db_b").unwrap();
        let handle = tokio::spawn(waiter.into_future());

        // Not resolved while armed.
        assert!(!handle.is_finished());

        stop("omni_test_db_b");
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve after stop")
            .unwrap();
        assert!(!is_on("omni_test_db_b"));
    }

    #[tokio::test]
    async fn test_barrier_is_per_database() {
        start("omni_test_db_c");
        assert!(waiter("omni_test_db_other").is_none());
        stop("omni_test_db_c");
    }

    #[tokio::test]
    async fn test_rearm_keeps_waiters_parked() {
        start("omni_test_db_d");
        let waiter = waiter("omni_test_db_d").unwrap();
        let handle = tokio::spawn(waiter.into_future());

        // Release and immediately re-arm: the waiter re-checks and
        // stays parked.
        stop("omni_test_db_d");
        start("omni_test_db_d");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        stop("omni_test_db_d");
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve after final stop")
            .unwrap();
    }

    #[tokio::test]
    async fn test_double_start_keeps_existing_waiters() {
        start("omni_test_db_e");
        let waiter = waiter("omni_test_db_e").unwrap();
        let handle = tokio::spawn(waiter.into_future());

        // Re-arming while already armed must not orphan waiters.
        start("omni_test_db_e");
        stop("omni_test_db_e");

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve")
            .unwrap();
    }
}
