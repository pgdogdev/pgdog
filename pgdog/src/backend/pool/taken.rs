use std::collections::hash_map::Entry;
use std::sync::Arc;

use fnv::FnvHashMap as HashMap;
use tokio::sync::Notify;

use crate::backend::pool::cancel::{BackendCancelState, CancelLease, CancelLeaseReleaseOutcome};
use crate::net::{BackendKeyData, BackendPid, FrontendPid};

use super::Error;
use super::Pool;

/// Bundles the backend's identity with the cancel key it carries.
#[derive(Clone, Debug)]
struct Checkout {
    backend: BackendPid,
    key: BackendKeyData,
    /// The client has pinned this checkout (e.g. holds an advisory lock),
    /// so it must not be recycled back into transaction pooling until the
    /// pin is released.
    locked: bool,
}

/// Track the link between a frontend connection and the backend connection it
/// currently holds, so cancel requests can be routed.
///
/// A Postgres CancelRequest carries only the frontend's identity; it has no
/// way to name the backend connection pgdog assigned to that frontend. This
/// struct stores that mapping for the pool's checked-out connections.
#[derive(Default, Clone, Debug)]
pub(super) struct Taken {
    /// Frontend pid -> checkout info for the backend connection currently
    /// assigned to that frontend. Cancel routing reads this directly.
    frontend_to_cancel: HashMap<FrontendPid, Checkout>,
    /// Reverse index from backend pid to the frontend pid that holds it. On
    /// check-in the pool only knows the backend pid, so we use this to find
    /// which `frontend_to_cancel` entry to drop.
    backend_to_frontend: HashMap<BackendPid, FrontendPid>,
    /// Cancel leases outstanding per backend pid.
    backend_cancels_in_flight: HashMap<BackendPid, BackendCancelState>,
}

impl Taken {
    #[inline]
    pub(super) fn take(&mut self, frontend: FrontendPid, backend: BackendPid, key: BackendKeyData) {
        self.backend_to_frontend.insert(backend, frontend);
        self.frontend_to_cancel.insert(
            frontend,
            Checkout {
                backend,
                key,
                locked: false,
            },
        );
    }

    /// Clear the checkout tracking for a backend being returned to the pool.
    ///
    /// Returns [`Error::UntrackedConnCheckin`] if the backend was never
    /// tracked — a double-checkin or a checkin for a backend that never went
    /// through [`Self::take`].
    #[inline]
    pub(super) fn check_in(&mut self, backend: BackendPid) -> Result<(), Error> {
        let frontend = self
            .backend_to_frontend
            .remove(&backend)
            .ok_or(Error::UntrackedConnCheckin(backend))?;
        // Drop the frontend's cancel entry only when it still names this
        // backend. The deferred check-in from a prior `Server::drop` may fire
        // after the frontend has already taken a newer backend; in that case
        // the entry belongs to the newer backend and must not be touched.
        if let Entry::Occupied(entry) = self.frontend_to_cancel.entry(frontend)
            && entry.get().backend == backend
        {
            entry.remove();
        }
        Ok(())
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.backend_to_frontend.len()
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.backend_to_frontend.is_empty()
    }

    /// Backend cancel key for this frontend's current checkout.
    #[cfg(test)]
    #[inline]
    pub(super) fn cancel_key(&self, frontend: FrontendPid) -> Option<&BackendKeyData> {
        self.frontend_to_cancel.get(&frontend).map(|c| &c.key)
    }

    /// All cancel keys for currently checked-out backend connections. For
    /// frontends with multiple concurrent checkouts, only the latest is
    /// returned (matches prior behavior).
    pub(super) fn cancel_keys(&self) -> impl Iterator<Item = &BackendKeyData> {
        self.frontend_to_cancel.values().map(|c| &c.key)
    }

    /// Increment the in-flight counter for `frontend`'s backend and hand
    /// back a [`CancelLease`] that releases on drop.
    #[inline]
    pub(super) fn begin_cancel(
        &mut self,
        frontend: FrontendPid,
        pool: &Pool,
    ) -> Option<CancelLease> {
        let entry = self.frontend_to_cancel.get(&frontend)?;
        let backend = entry.backend;
        let key = entry.key.clone();
        let state = self
            .backend_cancels_in_flight
            .entry(backend)
            .or_insert_with(|| BackendCancelState {
                count: 0,
                notify: Arc::new(Notify::new()),
            });

        state.count += 1;

        Some(CancelLease::new(pool.clone(), backend, key))
    }

    /// Release a cancel lease previously acquired with [`Self::begin_cancel`].
    /// Fires the per-backend `Notify` when the last lease drops so parked
    /// check-ins for that specific backend wake up — other backends'
    /// waiters stay asleep.
    #[inline]
    pub(super) fn end_cancel(&mut self, backend: BackendPid) -> CancelLeaseReleaseOutcome {
        match self.backend_cancels_in_flight.entry(backend) {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.count = state.count.saturating_sub(1);
                if state.count == 0 {
                    // Clone the Arc before removing the entry so the
                    // Notify stays alive for any waiter that already
                    // registered against it.
                    let notify = state.notify.clone();
                    entry.remove();
                    notify.notify_waiters();
                    CancelLeaseReleaseOutcome::Cleared
                } else {
                    CancelLeaseReleaseOutcome::StillPending
                }
            }
            Entry::Vacant(_) => CancelLeaseReleaseOutcome::NotTracked,
        }
    }

    /// True if any cancel packet targeting `backend` is still in flight.
    /// Check-in and reassignment must wait while this is the case.
    #[inline]
    pub(super) fn does_backend_have_pending_cancel(&self, backend: BackendPid) -> bool {
        self.backend_cancels_in_flight.contains_key(&backend)
    }

    /// Handle to the per-backend `Notify` that fires when the last lease
    /// on `backend` drops. `None` if no cancel is currently in flight for
    /// `backend`.
    #[inline]
    pub(super) fn get_cancel_notify(&self, backend: BackendPid) -> Option<Arc<Notify>> {
        self.backend_cancels_in_flight
            .get(&backend)
            .map(|s| s.notify.clone())
    }

    /// True if any backend has an outstanding cancel lease. `Pool::move_conns_to`
    /// uses this to refuse draining while leases still target this pool.
    #[inline]
    pub(super) fn has_any_cancels_in_flight(&self) -> bool {
        !self.backend_cancels_in_flight.is_empty()
    }

    /// Mark or unmark a checked-out backend as pinned to its client. Called by
    /// the frontend when it takes/releases an advisory lock or manual pin.
    #[inline]
    pub(super) fn set_locked(&mut self, backend: BackendPid, locked: bool) {
        if let Some(&frontend) = self.backend_to_frontend.get(&backend)
            && let Some(entry) = self.frontend_to_cancel.get_mut(&frontend)
            && entry.backend == backend
        {
            entry.locked = locked;
        }
    }

    /// Count checked-out backends currently pinned to their client.
    #[inline]
    pub(super) fn locked_count(&self) -> usize {
        self.frontend_to_cancel
            .values()
            .filter(|c| c.locked)
            .count()
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::Pool;
    use crate::backend::pool::cancel::CancelLease;

    fn key(pid: i32) -> BackendKeyData {
        BackendKeyData::legacy(pid, 0)
    }

    #[test]
    fn empty_state_has_no_entries() {
        let taken = Taken::default();
        assert_eq!(taken.len(), 0);
        assert!(taken.is_empty());
        assert_eq!(taken.cancel_key(FrontendPid::new()), None);
        assert_eq!(taken.cancel_keys().count(), 0);
    }

    #[test]
    fn take_then_check_in_round_trip() {
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(1);
        let cancel_key = key(backend.pid);

        taken.take(frontend, backend, cancel_key.clone());
        assert_eq!(taken.len(), 1);
        assert_eq!(taken.cancel_key(frontend), Some(&cancel_key));
        assert_eq!(taken.cancel_keys().count(), 1);

        taken.check_in(backend).unwrap();
        assert!(taken.is_empty());
        assert_eq!(taken.cancel_key(frontend), None);
    }

    #[test]
    fn check_in_unknown_backend_errors() {
        let mut taken = Taken::default();
        let unknown = BackendPid::for_test(99);
        assert_eq!(
            taken.check_in(unknown).unwrap_err(),
            Error::UntrackedConnCheckin(unknown),
        );
    }

    #[test]
    fn cancel_key_recovers_server_pid() {
        // The map relies on cancel_key.pid() == backend pid as an invariant.
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(2);

        taken.take(frontend, backend, key(backend.pid));
        assert_eq!(
            taken.cancel_key(frontend).map(|k| k.pid()),
            Some(backend.pid)
        );
    }

    #[test]
    fn distinct_frontends_are_independent() {
        let mut taken = Taken::default();
        let (fa, ba) = (FrontendPid::new(), BackendPid::for_test(3));
        let (fb, bb) = (FrontendPid::new(), BackendPid::for_test(4));

        taken.take(fa, ba, key(ba.pid));
        taken.take(fb, bb, key(bb.pid));
        assert_eq!(taken.len(), 2);

        taken.check_in(ba).unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(taken.cancel_key(fa), None);
        assert_eq!(taken.cancel_key(fb).map(|k| k.pid()), Some(bb.pid));

        taken.check_in(bb).unwrap();
        assert!(taken.is_empty());
    }

    /// Regression: the `Server::drop` race documented on `check_in`.
    ///
    /// Sequence reproduced here:
    ///   1. Frontend F takes backend A.
    ///   2. F's guard drops; `Server::drop` defers the check-in to a tokio task.
    ///   3. Before that task runs, F takes backend B (entry for F overwritten).
    ///   4. The deferred check-in for A finally fires.
    ///
    /// After step 4, F is still actively using B, so cancel routing for F
    /// must still resolve to B. Final check-in of B clears everything.
    #[test]
    fn deferred_check_in_after_same_frontend_retake() {
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend_a = BackendPid::for_test(5);
        let backend_b = BackendPid::for_test(6);
        let key_a = key(backend_a.pid);
        let key_b = key(backend_b.pid);

        // Step 1: take A.
        taken.take(frontend, backend_a, key_a.clone());
        assert_eq!(taken.cancel_key(frontend), Some(&key_a));

        // Step 3: F retakes with B before A's deferred check-in fires.
        taken.take(frontend, backend_b, key_b.clone());
        assert_eq!(taken.len(), 2, "both backends still tracked");
        assert_eq!(taken.cancel_key(frontend), Some(&key_b), "latest wins");

        // Step 4: deferred check-in for A. Must NOT touch F's entry,
        // since it now belongs to B.
        taken.check_in(backend_a).unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(
            taken.cancel_key(frontend),
            Some(&key_b),
            "cancel routing for F must still target the live backend B",
        );

        // Normal check-in of B clears the entry.
        taken.check_in(backend_b).unwrap();
        assert!(taken.is_empty());
        assert_eq!(taken.cancel_key(frontend), None);
    }

    /// Reverse order of the race: A's deferred check-in fires *before* F
    /// retakes. Sanity check that the normal path still works.
    #[test]
    fn deferred_check_in_before_same_frontend_retake() {
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend_a = BackendPid::for_test(7);
        let backend_b = BackendPid::for_test(8);

        taken.take(frontend, backend_a, key(backend_a.pid));
        taken.check_in(backend_a).unwrap();
        assert!(taken.is_empty());

        taken.take(frontend, backend_b, key(backend_b.pid));
        assert_eq!(
            taken.cancel_key(frontend).map(|k| k.pid()),
            Some(backend_b.pid)
        );
        taken.check_in(backend_b).unwrap();
        assert!(taken.is_empty());
    }

    #[test]
    fn set_locked_toggles_bit_and_counts() {
        let mut taken = Taken::default();
        let (fa, ba) = (FrontendPid::new(), BackendPid::for_test(10));
        let (fb, bb) = (FrontendPid::new(), BackendPid::for_test(11));

        taken.take(fa, ba, key(ba.pid));
        taken.take(fb, bb, key(bb.pid));
        assert_eq!(taken.locked_count(), 0);

        taken.set_locked(ba, true);
        assert_eq!(taken.locked_count(), 1);

        taken.set_locked(bb, true);
        assert_eq!(taken.locked_count(), 2);

        taken.set_locked(ba, false);
        assert_eq!(taken.locked_count(), 1);
    }

    #[test]
    fn check_in_clears_locked_state() {
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(12);

        taken.take(frontend, backend, key(backend.pid));
        taken.set_locked(backend, true);
        assert_eq!(taken.locked_count(), 1);

        taken.check_in(backend).unwrap();
        assert_eq!(taken.locked_count(), 0);
    }

    #[test]
    fn set_locked_unknown_backend_is_noop() {
        let mut taken = Taken::default();
        let unknown = BackendPid::for_test(13);

        taken.set_locked(unknown, true);
        assert_eq!(taken.locked_count(), 0);
    }

    /// After a retake race, a stale backend's `set_locked` call must NOT flip
    /// the current live backend's bit (see `deferred_check_in_after_same_frontend_retake`).
    #[test]
    fn set_locked_after_retake_race_ignores_stale_backend() {
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend_a = BackendPid::for_test(14);
        let backend_b = BackendPid::for_test(15);

        taken.take(frontend, backend_a, key(backend_a.pid));
        taken.take(frontend, backend_b, key(backend_b.pid));

        // Stale A tries to set locked. Must not affect B.
        taken.set_locked(backend_a, true);
        assert_eq!(taken.locked_count(), 0);

        // Live B still works normally.
        taken.set_locked(backend_b, true);
        assert_eq!(taken.locked_count(), 1);
    }

    #[test]
    fn begin_cancel_returns_none_without_checkout() {
        let pool = Pool::new_test();
        assert!(CancelLease::acquire(&pool, FrontendPid::new()).is_none());
    }

    #[test]
    fn begin_cancel_snapshots_backend_and_key() {
        let pool = Pool::new_test();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(20);
        let cancel_key = key(backend.pid);
        pool.lock()
            .taken
            .take(frontend, backend, cancel_key.clone());

        let lease = CancelLease::acquire(&pool, frontend).unwrap();
        assert_eq!(lease.backend(), backend);
        assert_eq!(lease.key, cancel_key);
        assert!(pool.lock().taken.does_backend_have_pending_cancel(backend));
    }

    #[test]
    fn end_cancel_clears_when_last_lease_drops() {
        let pool = Pool::new_test();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(21);
        pool.lock().taken.take(frontend, backend, key(backend.pid));

        let lease1 = CancelLease::acquire(&pool, frontend).unwrap();
        let lease2 = CancelLease::acquire(&pool, frontend).unwrap();
        assert!(pool.lock().taken.does_backend_have_pending_cancel(backend));

        drop(lease1);
        assert!(
            pool.lock().taken.does_backend_have_pending_cancel(backend),
            "one lease still outstanding",
        );

        drop(lease2);
        assert!(
            !pool.lock().taken.does_backend_have_pending_cancel(backend),
            "last lease released",
        );
    }

    /// Multiple concurrent cancels for the same frontend must stack on the
    /// same backend's counter. Every intermediate drop leaves the backend
    /// pinned; only the final drop clears it. This is the property that
    /// makes concurrent `pg_cancel` calls from a client (or from separate
    /// callers targeting the same session) all wait for each other before
    /// the backend can be reassigned.
    #[test]
    fn cancel_leases_stack_on_same_backend() {
        let pool = Pool::new_test();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(40);
        pool.lock().taken.take(frontend, backend, key(backend.pid));

        // Stack four concurrent cancels for the same frontend.
        let l1 = CancelLease::acquire(&pool, frontend).unwrap();
        let l2 = CancelLease::acquire(&pool, frontend).unwrap();
        let l3 = CancelLease::acquire(&pool, frontend).unwrap();
        let l4 = CancelLease::acquire(&pool, frontend).unwrap();

        // All four target the same physical backend.
        assert_eq!(l1.backend(), backend);
        assert_eq!(l2.backend(), backend);
        assert_eq!(l3.backend(), backend);
        assert_eq!(l4.backend(), backend);

        // The counter reflects the stack depth and the backend stays
        // pinned throughout every intermediate drop.
        assert!(pool.lock().taken.does_backend_have_pending_cancel(backend));

        drop(l1);
        assert!(
            pool.lock().taken.does_backend_have_pending_cancel(backend),
            "3 leases still stacked",
        );

        // Drop out of order to prove the counter is order-agnostic.
        drop(l3);
        assert!(
            pool.lock().taken.does_backend_have_pending_cancel(backend),
            "2 leases still stacked",
        );

        drop(l2);
        assert!(
            pool.lock().taken.does_backend_have_pending_cancel(backend),
            "1 lease still stacked",
        );

        drop(l4);
        assert!(
            !pool.lock().taken.does_backend_have_pending_cancel(backend),
            "last lease released, backend fully unpinned",
        );

        // After the whole stack unwinds, `end_cancel` on this backend
        // returns `NotTracked` — the counter entry was removed by the
        // final decrement.
        assert_eq!(
            pool.lock().taken.end_cancel(backend),
            CancelLeaseReleaseOutcome::NotTracked,
        );
    }

    /// The RAII drop of [`CancelLease`] must run on every exit path,
    /// including `?`-propagated errors from `Server::cancel`. Simulate the
    /// early-return-with-error shape and verify the counter is released.
    #[test]
    fn cancel_lease_releases_counter_on_error_early_return() {
        let pool = Pool::new_test();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(30);
        pool.lock().taken.take(frontend, backend, key(backend.pid));

        fn simulate_cancel_that_errors(
            pool: &Pool,
            client: FrontendPid,
        ) -> Result<(), &'static str> {
            let _lease = CancelLease::acquire(pool, client).ok_or("no checkout")?;
            // Pretend `Server::cancel(...).await?` errored here — early
            // return before any explicit `drop(lease)`. Rust must still
            // run the lease's Drop as `_lease` goes out of scope.
            Err("simulated Server::cancel failure")
        }

        assert!(simulate_cancel_that_errors(&pool, frontend).is_err());
        assert!(
            !pool.lock().taken.does_backend_have_pending_cancel(backend),
            "counter must decrement on the error-propagation path too",
        );
    }

    #[test]
    fn end_cancel_on_unknown_backend_is_not_tracked() {
        // `end_cancel` operates purely on the counter, no lease needed —
        // still worth testing directly at the Taken level so we know the
        // enum discriminant for the misuse case.
        let mut taken = Taken::default();
        let unknown = BackendPid::for_test(22);
        assert_eq!(
            taken.end_cancel(unknown),
            CancelLeaseReleaseOutcome::NotTracked
        );
    }

    /// If the frontend retakes a different backend between begin_cancel and
    /// lease drop, the lease still names the original backend and must
    /// decrement that backend's counter — not the frontend's current one.
    #[test]
    fn end_cancel_targets_original_backend_across_retake() {
        let pool = Pool::new_test();
        let frontend = FrontendPid::new();
        let backend_a = BackendPid::for_test(23);
        let backend_b = BackendPid::for_test(24);

        pool.lock()
            .taken
            .take(frontend, backend_a, key(backend_a.pid));
        let lease = CancelLease::acquire(&pool, frontend).unwrap();
        assert_eq!(lease.backend(), backend_a);

        // Frontend retakes with a different backend.
        pool.lock()
            .taken
            .take(frontend, backend_b, key(backend_b.pid));

        // Lease drop still targets A, not B.
        drop(lease);
        assert!(
            !pool
                .lock()
                .taken
                .does_backend_have_pending_cancel(backend_a)
        );
        assert!(
            !pool
                .lock()
                .taken
                .does_backend_have_pending_cancel(backend_b)
        );
    }

    #[test]
    fn double_check_in_second_errors() {
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(9);

        taken.take(frontend, backend, key(backend.pid));
        taken.check_in(backend).unwrap();
        assert_eq!(
            taken.check_in(backend).unwrap_err(),
            Error::UntrackedConnCheckin(backend),
        );
    }
}
