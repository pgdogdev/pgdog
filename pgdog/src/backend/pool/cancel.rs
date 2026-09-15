//! Frontend-scoped cancellation.
//!
//! A Postgres `CancelRequest` carries only `(backend_pid, secret)`. In
//! transaction pool mode the backend behind a frontend can be reassigned
//! between the snapshot of that pair and the moment the packet lands on
//! the wire, so a stale cancel key can hit a different frontend's query.
//! [`CancelLease`] pins the physical backend for the duration of the
//! send; check-in parks on a per-backend `Notify` (in
//! [`super::taken::Taken`]) until every outstanding lease against that
//! backend has dropped.

use std::sync::Arc;
use tokio::sync::Notify;
use tracing::error;

use crate::backend::Server;
use crate::net::messages::{BackendKeyData, BackendPid, FrontendPid};

use super::Pool;

/// Pins a physical backend against reassignment while a CancelRequest
/// targeting it is in flight. Acquire with [`CancelLease::acquire`];
/// release is automatic on drop.
pub struct CancelLease {
    pool: Pool,
    backend: BackendPid,
    pub key: BackendKeyData,
}

impl CancelLease {
    /// Acquire a lease against the backend `client` currently holds.
    /// Returns `None` if `client` has no checkout.
    ///
    /// Bumps the pool's cumulative `cancels` counter for every lease that
    /// actually gets issued, so downstream metrics (SHOW POOLS, Prometheus)
    /// can report how many CancelRequests this pool has dispatched.
    pub fn acquire(pool: &Pool, client: FrontendPid) -> Option<Self> {
        let mut inner = pool.lock();
        let lease = inner.begin_cancel(client, pool)?;
        inner.stats.counts.cancels += 1;
        Some(lease)
    }

    pub(super) fn new(pool: Pool, backend: BackendPid, key: BackendKeyData) -> Self {
        Self { pool, backend, key }
    }

    #[cfg(test)]
    pub(super) fn backend(&self) -> BackendPid {
        self.backend
    }
}

impl Drop for CancelLease {
    fn drop(&mut self) {
        // `Taken::end_cancel` fires the per-backend Notify when the count
        // reaches zero — no explicit `notify_waiters` needed here.
        match self.pool.lock().end_cancel(self.backend) {
            CancelLeaseReleaseOutcome::Cleared | CancelLeaseReleaseOutcome::StillPending => {}
            CancelLeaseReleaseOutcome::NotTracked => {
                error!(
                    "cancel lease drop found no counter for backend {:?}",
                    self.backend,
                );
                #[cfg(debug_assertions)]
                panic!(
                    "CancelLease::drop found no counter for backend {:?}",
                    self.backend,
                );
            }
        }
    }
}

/// Per-backend cancel-lease state. Kept in a map keyed by `BackendPid` so
/// waiters wake only when *their* backend's leases have all cleared, not
/// when any cancel anywhere in the pool completes.
#[derive(Clone, Debug)]
pub struct BackendCancelState {
    /// Number of `CancelLease`s currently pinning this backend.
    pub count: u32,
    /// Wake signal fired when `count` transitions to zero. `Arc` so a
    /// parked waiter can keep the `Notify` alive after the map entry has
    /// been removed by the last `end_cancel`.
    pub notify: Arc<Notify>,
}

impl Pool {
    /// Cancel the query the given frontend is currently running.
    ///
    /// The physical backend stays pinned via [`CancelLease`] until Postgres
    /// has finished processing the CancelRequest, so a racing reassignment
    /// can't redirect the cancel to another frontend.
    pub async fn cancel(&self, id: FrontendPid) -> Result<(), crate::backend::Error> {
        let Some(lease) = CancelLease::acquire(self, id) else {
            return Ok(());
        };
        Server::cancel(self.addr(), lease.key.clone()).await?;
        drop(lease);
        Ok(())
    }

    /// Park until every cancel lease targeting `backend` has been released.
    /// Called by the deferred check-in path so a returning backend isn't
    /// reassigned while its cancel is still being processed by Postgres.
    pub(super) async fn wait_for_cancels_to_finish(&self, backend: BackendPid) {
        loop {
            let notify = match self.lock().taken.get_cancel_notify(backend) {
                Some(notify) => notify,
                None => return,
            };
            let notified = notify.notified();
            tokio::pin!(notified);
            let still_pending = {
                let inner = self.lock();
                notified.as_mut().enable();
                inner.does_backend_have_pending_cancel(backend)
            };
            if !still_pending {
                break;
            }
            notified.as_mut().await;
        }
    }
}

/// Outcome of releasing a cancel lease via [`Taken::end_cancel`].
///
/// `Cleared` and `StillPending` are the normal paths; `NotTracked` means
/// the caller decremented a backend that had no counter, indicating a bug (double
/// release, or `end_cancel` called against a backend that never had a
/// lease). Callers use this to decide whether to notify parked check-ins
/// and to surface the misuse case.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) enum CancelLeaseReleaseOutcome {
    /// The last outstanding lease on this backend just dropped. Parked
    /// check-ins for this backend should be woken.
    Cleared,
    /// Other cancel leases on this backend are still in flight. Nothing
    /// to wake.
    StillPending,
    /// No lease was tracked for this backend. error.
    NotTracked,
}
