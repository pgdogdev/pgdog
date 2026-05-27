//! Pause access to all/specific databases while we change the world.
//!
//! Maintenance mode is special: it's completely independent from the config
//! and will hold true during config changes, e.g. when some databases disappear
//! and new ones are added.
//!
//! This is useful when changing the sharding config online, for example.
//!
use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tracing::warn;

static MAINTENANCE_MODE: Lazy<MaintenanceMode> = Lazy::new(|| MaintenanceMode {
    all_channel: Mutex::new(None),
    databases: Mutex::new(HashMap::new()),
    all: AtomicBool::new(false),
    active: AtomicUsize::new(0),
});

pub(crate) fn waiter(database: &str) -> Option<Waiter> {
    MAINTENANCE_MODE.get_waiter(database)
}

/// Future that resolves once a database leaves maintenance mode.
///
/// Wraps the broadcast receiver so callers can simply `.await` it; it resolves
/// when the maintenance channel is closed (the sender is dropped by `stop`).
pub(crate) struct Waiter {
    receiver: broadcast::Receiver<()>,
}

impl IntoFuture for Waiter {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // Resolves when the channel is closed (sender dropped).
            let _ = self.receiver.recv().await;
        })
    }
}

pub fn start(database: Option<&str>) {
    match database {
        Some(database) => {
            MAINTENANCE_MODE.add(database);
            warn!("maintenance mode is on for database \"{}\"", database);
        }
        None => {
            MAINTENANCE_MODE.add_all();
            warn!("maintenance mode is on for all databases");
        }
    }
}

pub fn stop(database: Option<&str>) {
    match database {
        Some(database) => {
            MAINTENANCE_MODE.remove(database);
            warn!("maintenance mode is off for database \"{}\"", database);
        }
        None => {
            MAINTENANCE_MODE.remove_all();
            warn!("maintenance mode is off for all databases");
        }
    }
}

#[cfg(test)]
pub fn is_on(database: &str) -> bool {
    MAINTENANCE_MODE.paused(database)
}

#[derive(Debug)]
struct MaintenanceMode {
    /// Channel for `all`-maintenance. Present (`Some`) while it's on; dropping
    /// it closes the channel and wakes every waiter.
    all_channel: Mutex<Option<broadcast::Sender<()>>>,
    /// One channel per individually-paused database. Removing a database drops
    /// its sender, closing the channel and waking its waiters.
    databases: Mutex<HashMap<String, broadcast::Sender<()>>>,
    all: AtomicBool,
    active: AtomicUsize,
}

impl MaintenanceMode {
    /// Check whether the given database is currently in maintenance mode. This is very fast for the common case (off),
    /// and pretty fast for not so common one when one of the databases is in maintenance mode.
    ///
    /// # Arguments
    ///
    /// * `database`: name of the database to check.
    ///
    /// # Return
    ///
    /// True if the database is in maintenance mode, false otherwise.
    #[inline]
    fn paused(&self, database: &str) -> bool {
        self.all.load(Ordering::Relaxed)
            || (self.active.load(Ordering::Relaxed) != 0
                && self.databases.lock().contains_key(database))
    }

    /// Get a [`Waiter`] that resolves once the database leaves maintenance
    /// mode, or `None` if it isn't in maintenance mode right now.
    ///
    /// # Arguments
    ///
    /// * `database`: name of the database to wait for.
    ///
    fn get_waiter(&self, database: &str) -> Option<Waiter> {
        // Fast path: no subscription unless this database is actually paused.
        if !self.paused(database) {
            return None;
        }

        let receiver = match self.databases.lock().get(database) {
            Some(sender) => Some(sender.subscribe()),
            None => self
                .all_channel
                .lock()
                .as_ref()
                .map(|sender| sender.subscribe()),
        };

        receiver.map(|receiver| Waiter { receiver })
    }

    /// Put a single database into maintenance mode.
    ///
    /// # Arguments
    ///
    /// * `database`: name of the database to pause.
    ///
    fn add(&self, database: &str) {
        let mut databases = self.databases.lock();
        // Keep the existing channel if already paused, so current waiters
        // stay valid.
        databases
            .entry(database.to_string())
            .or_insert_with(|| broadcast::channel(1).0);
        self.active.store(databases.len(), Ordering::Relaxed);
    }

    /// Take a single database out of maintenance mode and wake its waiters by
    /// dropping (closing) its channel.
    ///
    /// # Arguments
    ///
    /// * `database`: name of the database to resume.
    ///
    fn remove(&self, database: &str) {
        let mut databases = self.databases.lock();
        databases.remove(database);
        self.active.store(databases.len(), Ordering::Relaxed);
    }

    /// Put every database into maintenance mode, including ones that aren't
    /// connected yet.
    fn add_all(&self) {
        let mut all_channel = self.all_channel.lock();
        if all_channel.is_none() {
            *all_channel = Some(broadcast::channel(1).0);
        }
        self.all.store(true, Ordering::Relaxed);
    }

    /// Take every database out of `all`-maintenance and wake its waiters by
    /// dropping (closing) the channel. Databases paused individually stay
    /// paused.
    fn remove_all(&self) {
        self.all.store(false, Ordering::Relaxed);
        self.all_channel.lock().take();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Fresh, isolated instance so tests don't share the global singleton.
    fn maintenance() -> MaintenanceMode {
        MaintenanceMode {
            all_channel: Mutex::new(None),
            databases: Mutex::new(HashMap::new()),
            all: AtomicBool::new(false),
            active: AtomicUsize::new(0),
        }
    }

    #[test]
    fn nothing_paused_by_default() {
        let m = maintenance();
        assert!(!m.paused("anything"));
        assert!(m.get_waiter("anything").is_none());
    }

    #[test]
    fn pause_single_database() {
        let m = maintenance();
        m.add("one");

        assert!(m.paused("one"));
        assert!(!m.paused("two"));
        assert_eq!(m.active.load(Ordering::Relaxed), 1);

        // Only the paused database gets a waiter.
        assert!(m.get_waiter("one").is_some());
        assert!(m.get_waiter("two").is_none());

        m.remove("one");
        assert!(!m.paused("one"));
        assert_eq!(m.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn pause_is_idempotent() {
        let m = maintenance();
        m.add("one");
        m.add("one");
        assert_eq!(m.active.load(Ordering::Relaxed), 1);

        m.remove("one");
        assert_eq!(m.active.load(Ordering::Relaxed), 0);
        // Removing again is a no-op.
        m.remove("one");
        assert_eq!(m.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn pause_all_databases() {
        let m = maintenance();
        m.add_all();

        assert!(m.paused("one"));
        assert!(m.paused("two"));
        assert!(m.get_waiter("anything").is_some());

        m.remove_all();
        assert!(!m.paused("one"));
        assert!(!m.paused("two"));
    }

    #[test]
    fn remove_all_keeps_individual_pauses() {
        let m = maintenance();
        m.add("one");
        m.add_all();

        assert!(m.paused("one"));
        assert!(m.paused("two"));

        // Lifting `all` leaves individually-paused databases paused.
        m.remove_all();
        assert!(m.paused("one"));
        assert!(!m.paused("two"));
    }

    #[tokio::test]
    async fn waiter_pending_until_resumed() {
        let m = maintenance();
        m.add("one");
        let waiter = m.get_waiter("one").expect("database is paused");

        // While paused, the waiter does not resolve.
        let pending = timeout(Duration::from_millis(100), waiter.into_future()).await;
        assert!(pending.is_err(), "waiter should still be pending");
    }

    #[tokio::test]
    async fn waiter_resolves_on_remove() {
        let m = maintenance();
        m.add("one");
        let waiter = m.get_waiter("one").expect("database is paused");

        m.remove("one");

        timeout(Duration::from_secs(1), waiter.into_future())
            .await
            .expect("waiter should resolve once the database is resumed");
    }

    #[tokio::test]
    async fn waiter_resolves_on_remove_all() {
        let m = maintenance();
        m.add_all();
        let waiter = m.get_waiter("one").expect("all databases are paused");

        m.remove_all();

        timeout(Duration::from_secs(1), waiter.into_future())
            .await
            .expect("waiter should resolve once maintenance is lifted");
    }

    #[tokio::test]
    async fn waiter_survives_re_pause() {
        // Re-pausing an already-paused database must keep the existing channel,
        // so a waiter created before the second `add` still resolves on remove.
        let m = maintenance();
        m.add("one");
        let waiter = m.get_waiter("one").expect("database is paused");
        m.add("one"); // must not replace the channel

        m.remove("one");

        timeout(Duration::from_secs(1), waiter.into_future())
            .await
            .expect("waiter should resolve even after a re-pause");
    }
}
