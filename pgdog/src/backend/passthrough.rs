//! Throttle for passthrough credential verification.
//!
//! Verifying a credential costs a real connection to PostgreSQL (see
//! [`databases::add_passthrough`](super::databases::add_passthrough)), and the
//! client decides when that happens: every login with a credential the pool
//! has not seen before triggers one. Two things keep that bounded.
//!
//! * A credential the server rejected is remembered for a few seconds, so an
//!   application reconnecting in a loop with a wrong password costs one server
//!   connection rather than one per attempt. Only that exact credential is
//!   remembered, so a client that fixes its password is never locked out.
//! * Verifications that do go out are capped, so a burst of logins cannot open
//!   an unbounded number of server connections at once. Logins beyond the cap
//!   wait instead of adding load.

use std::{
    collections::HashMap,
    hash::{BuildHasher, RandomState},
    time::Duration,
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::{
    sync::{Semaphore, SemaphorePermit},
    time::Instant,
};

/// How long a rejected credential is remembered.
const REJECTED_FOR: Duration = Duration::from_secs(5);

/// Most verifications allowed to be in flight at once.
const MAX_IN_FLIGHT: usize = 8;

/// Most rejections remembered at once. A client that varies the user name on
/// every attempt must not be able to grow this without bound; past the cap,
/// rejections simply are not remembered.
const MAX_REJECTED: usize = 1024;

static THROTTLE: Lazy<Throttle> = Lazy::new(Throttle::default);

/// The process-wide verification throttle.
pub(crate) fn throttle() -> &'static Throttle {
    &THROTTLE
}

/// Identifies one (user, database, credential) attempt.
///
/// The credential is kept as a hash: a rejected password is often a valid
/// password somewhere else, and there is no reason to hold on to it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Attempt(u64);

impl Attempt {
    pub(crate) fn new(user: &str, database: &str, credential: &str) -> Self {
        static HASHER: Lazy<RandomState> = Lazy::new(RandomState::new);
        Self(HASHER.hash_one((user, database, credential)))
    }
}

pub(crate) struct Throttle {
    rejected: Mutex<HashMap<Attempt, Instant>>,
    in_flight: Semaphore,
}

impl Default for Throttle {
    fn default() -> Self {
        Self {
            rejected: Mutex::new(HashMap::new()),
            in_flight: Semaphore::new(MAX_IN_FLIGHT),
        }
    }
}

impl Throttle {
    /// Was this exact credential rejected by the server just now?
    pub(crate) fn rejected(&self, attempt: &Attempt) -> bool {
        let mut rejected = self.rejected.lock();
        match rejected.get(attempt) {
            Some(at) if at.elapsed() < REJECTED_FOR => true,
            Some(_) => {
                rejected.remove(attempt);
                false
            }
            None => false,
        }
    }

    /// Remember that the server rejected this credential.
    pub(crate) fn reject(&self, attempt: Attempt) {
        let mut rejected = self.rejected.lock();
        let now = Instant::now();
        rejected.retain(|_, at| now.duration_since(*at) < REJECTED_FOR);

        if rejected.len() < MAX_REJECTED {
            rejected.insert(attempt, now);
        }
    }

    /// Wait for a slot to run a verification in. Held for the duration of the
    /// server connection attempt.
    pub(crate) async fn slot(&self) -> Option<SemaphorePermit<'_>> {
        self.in_flight.acquire().await.ok()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_only_the_same_credential_is_rejected() {
        let throttle = Throttle::default();
        let attempt = Attempt::new("alice", "pgdog", "wrong");

        assert!(!throttle.rejected(&attempt));
        throttle.reject(attempt);
        assert!(throttle.rejected(&attempt));

        // A corrected password is not held back by the previous rejection, and
        // neither is another user reusing the same one.
        assert!(!throttle.rejected(&Attempt::new("alice", "pgdog", "right")));
        assert!(!throttle.rejected(&Attempt::new("bob", "pgdog", "wrong")));
        assert!(!throttle.rejected(&Attempt::new("alice", "other", "wrong")));
    }

    #[tokio::test(start_paused = true)]
    async fn test_rejection_expires() {
        let throttle = Throttle::default();
        let attempt = Attempt::new("alice", "pgdog", "wrong");
        throttle.reject(attempt);

        tokio::time::advance(REJECTED_FOR + Duration::from_millis(1)).await;

        assert!(!throttle.rejected(&attempt));
        // The expired entry is dropped rather than kept around.
        assert!(throttle.rejected.lock().is_empty());
    }

    #[test]
    fn test_rejections_are_capped() {
        let throttle = Throttle::default();
        for i in 0..MAX_REJECTED + 10 {
            throttle.reject(Attempt::new(&format!("user{i}"), "pgdog", "wrong"));
        }

        assert_eq!(throttle.rejected.lock().len(), MAX_REJECTED);
    }

    #[tokio::test]
    async fn test_in_flight_verifications_are_capped() {
        let throttle = Throttle::default();
        let slots: Vec<_> = (0..MAX_IN_FLIGHT)
            .map(|_| throttle.slot())
            .collect::<Vec<_>>();
        let slots: Vec<_> = futures::future::join_all(slots).await;
        assert!(slots.iter().all(Option::is_some));

        // The next login has to wait for one of them.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), throttle.slot())
                .await
                .is_err()
        );

        drop(slots);
        assert!(throttle.slot().await.is_some());
    }
}
