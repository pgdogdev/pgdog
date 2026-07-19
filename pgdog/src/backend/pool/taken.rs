use std::collections::hash_map::Entry;

use fnv::FnvHashMap as HashMap;

use crate::net::{BackendKeyData, BackendPid, FrontendPid};

use super::Error;

/// Bundles the backend's identity with the cancel key it carries.
#[derive(Clone, Debug)]
struct Checkout {
    backend: BackendPid,
    key: BackendKeyData,
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
}

impl Taken {
    #[inline]
    pub(super) fn take(&mut self, frontend: FrontendPid, backend: BackendPid, key: BackendKeyData) {
        self.backend_to_frontend.insert(backend, frontend);
        self.frontend_to_cancel
            .insert(frontend, Checkout { backend, key });
    }

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

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let cancel_key = key(backend.pid());

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

        taken.take(frontend, backend, key(backend.pid()));
        assert_eq!(
            taken.cancel_key(frontend).map(|k| k.pid()),
            Some(backend.pid())
        );
    }

    #[test]
    fn distinct_frontends_are_independent() {
        let mut taken = Taken::default();
        let (fa, ba) = (FrontendPid::new(), BackendPid::for_test(3));
        let (fb, bb) = (FrontendPid::new(), BackendPid::for_test(4));

        taken.take(fa, ba, key(ba.pid()));
        taken.take(fb, bb, key(bb.pid()));
        assert_eq!(taken.len(), 2);

        taken.check_in(ba).unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(taken.cancel_key(fa), None);
        assert_eq!(taken.cancel_key(fb).map(|k| k.pid()), Some(bb.pid()));

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
        let key_a = key(backend_a.pid());
        let key_b = key(backend_b.pid());

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

        taken.take(frontend, backend_a, key(backend_a.pid()));
        taken.check_in(backend_a).unwrap();
        assert!(taken.is_empty());

        taken.take(frontend, backend_b, key(backend_b.pid()));
        assert_eq!(
            taken.cancel_key(frontend).map(|k| k.pid()),
            Some(backend_b.pid())
        );
        taken.check_in(backend_b).unwrap();
        assert!(taken.is_empty());
    }

    #[test]
    fn double_check_in_second_errors() {
        let mut taken = Taken::default();
        let frontend = FrontendPid::new();
        let backend = BackendPid::for_test(9);

        taken.take(frontend, backend, key(backend.pid()));
        taken.check_in(backend).unwrap();
        assert_eq!(
            taken.check_in(backend).unwrap_err(),
            Error::UntrackedConnCheckin(backend),
        );
    }
}
