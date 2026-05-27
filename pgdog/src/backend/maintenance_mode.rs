//! Pause access to all/specific databases while we change the world.
//!
//! Maintenance mode is special: it's independent from the config
//! and will hold true during config changes, e.g. when some databases disappear, e.g.,
//! replicas or shards are added/removed.
//!
//! This is useful when changing the sharding config online, for example.
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

static MAINTENANCE_MODE: Lazy<MaintenanceMode> = Lazy::new(|| MaintenanceMode {
    state: ArcSwap::from_pointee(MaintenanceState::default()),
    write_lock: Mutex::new(()),
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
    database: String,
}

impl IntoFuture for Waiter {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // Resolves when the channel is closed (sender dropped).
            let _ = self.receiver.recv().await;

            // Re-check to avoid race between MAINTENANCE ON <db> and MAINTENANCE ON.
            if let Some(waiter) = MAINTENANCE_MODE.get_waiter(&self.database) {
                let _ = waiter.await;
            }
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
    state: ArcSwap<MaintenanceState>,
    write_lock: Mutex<()>,
}

#[derive(Clone, Debug, Default)]
struct MaintenanceState {
    // Per-database maintenance mode.
    databases: HashMap<String, broadcast::Sender<()>>,
    // Global maintenance mode (all databases, current and future ones).
    all: Option<broadcast::Sender<()>>,
}

impl MaintenanceMode {
    /// Check whether the given database is currently in maintenance mode.
    #[cfg(test)]
    #[inline]
    fn paused(&self, database: &str) -> bool {
        self.get_waiter(database).is_some()
    }

    /// Get a [`Waiter`] that resolves once the database leaves maintenance
    /// mode, or `None` if it isn't in maintenance mode right now.
    ///
    /// # Arguments
    ///
    /// * `database`: name of the database to wait for.
    ///
    fn get_waiter(&self, database: &str) -> Option<Waiter> {
        let state = self.state.load();

        if state.databases.is_empty() && state.all.is_none() {
            return None;
        }

        match state.databases.get(database) {
            Some(sender) => Some(Waiter {
                receiver: sender.subscribe(),
                database: database.to_string(),
            }),
            None => state.all.as_ref().map(|sender| Waiter {
                receiver: sender.subscribe(),
                database: database.to_string(),
            }),
        }
    }

    /// Put a single database into maintenance mode.
    ///
    /// # Arguments
    ///
    /// * `database`: name of the database to pause.
    ///
    fn add(&self, database: &str) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = MaintenanceState::clone(&state);

        // Global maintenance covers individual databases.
        if next.all.is_some() {
            return;
        }

        // Keep the existing channel if already paused, so current waiters
        // stay valid.
        next.databases
            .entry(database.to_string())
            .or_insert_with(|| broadcast::channel(1).0);

        self.state.store(Arc::new(next));
    }

    /// Take a single database out of maintenance mode and wake its waiters by
    /// dropping (closing) its channel.
    ///
    /// # Arguments
    ///
    /// * `database`: name of the database to resume.
    ///
    fn remove(&self, database: &str) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = MaintenanceState::clone(&state);

        next.databases.remove(database);

        self.state.store(Arc::new(next));
    }

    /// Put every configured database into maintenance mode.
    fn add_all(&self) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();

        let mut next = MaintenanceState::clone(&state);

        if next.all.is_none() {
            next.all = Some(broadcast::channel(1).0);
        }

        self.state.store(Arc::new(next));
    }

    /// Take every database out of maintenance mode, including ones paused
    /// individually, and wake their waiters by dropping (closing) the channels.
    fn remove_all(&self) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();

        let mut next = MaintenanceState::clone(&state);

        next.all = None;
        next.databases.clear();

        self.state.store(Arc::new(next));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    /// Fresh, isolated instance so tests don't share the global singleton.
    fn maintenance() -> MaintenanceMode {
        MaintenanceMode {
            state: ArcSwap::from_pointee(MaintenanceState::default()),
            write_lock: Mutex::new(()),
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
        assert_eq!(m.state.load().databases.len(), 1);

        // Only the paused database gets a waiter.
        assert!(m.get_waiter("one").is_some());
        assert!(m.get_waiter("two").is_none());

        m.remove("one");
        assert!(!m.paused("one"));
        assert_eq!(m.state.load().databases.len(), 0);
    }

    #[test]
    fn pause_is_idempotent() {
        let m = maintenance();
        m.add("one");
        m.add("one");
        assert_eq!(m.state.load().databases.len(), 1);

        m.remove("one");
        assert_eq!(m.state.load().databases.len(), 0);
        // Removing again is a no-op.
        m.remove("one");
        assert_eq!(m.state.load().databases.len(), 0);
    }

    #[tokio::test]
    async fn pause_all_databases() {
        // `add_all` pauses every database in the loaded config ("pgdog").
        crate::config::load_test();
        let m = maintenance();
        m.add_all();

        assert!(m.paused("pgdog"));
        assert!(m.get_waiter("pgdog").is_some());

        m.remove_all();
        assert!(!m.paused("pgdog"));
    }

    #[tokio::test]
    async fn remove_all_clears_everything() {
        // `remove_all` clears the whole set, including databases paused
        // individually.
        crate::config::load_test();
        let m = maintenance();
        m.add("other");
        m.add_all(); // pauses the configured "pgdog"

        assert!(m.paused("pgdog"));
        assert!(m.paused("other"));

        m.remove_all();
        assert!(!m.paused("pgdog"));
        assert!(!m.paused("other"));
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
        crate::config::load_test();
        let m = maintenance();
        m.add_all();
        let waiter = m
            .get_waiter("pgdog")
            .expect("configured database is paused");

        m.remove_all();

        timeout(Duration::from_secs(1), waiter.into_future())
            .await
            .expect("waiter should resolve once maintenance is lifted");
    }

    #[tokio::test]
    async fn individual_maintenance_is_ignored_while_all_is_on() {
        let m = maintenance();
        m.add_all();
        m.add("db"); // no-op: `all` already covers "db"

        // "db" is paused, but only through the global `all` channel.
        assert!(m.state.load().databases.is_empty());
        let waiter = m
            .get_waiter("db")
            .expect("db is paused under all-maintenance");

        // Resuming "db" individually must NOT release it while `all` is on.
        m.remove("db");
        let pending = timeout(Duration::from_millis(100), waiter.into_future()).await;
        assert!(
            pending.is_err(),
            "db waiter must stay blocked while all-maintenance is on"
        );
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

    #[tokio::test]
    async fn individual_resume_does_not_release_under_global() {
        // Reverse ordering: a database is paused individually first, then global
        // maintenance is turned on. Resuming the database individually must not
        // release its waiter while global maintenance is still on — the waiter
        // re-checks and re-parks on the global channel.
        //
        // Drives the global instance because the waiter's re-check consults it.
        MAINTENANCE_MODE.remove_all(); // clean slate

        MAINTENANCE_MODE.add("db");
        let waiter = MAINTENANCE_MODE
            .get_waiter("db")
            .expect("db is paused individually");
        MAINTENANCE_MODE.add_all();

        let handle = tokio::spawn(async move { waiter.await });

        // Individual resume must not wake it while global maintenance is on.
        MAINTENANCE_MODE.remove("db");
        sleep(Duration::from_millis(50)).await;
        assert!(
            !handle.is_finished(),
            "waiter must stay blocked while global maintenance is on"
        );

        // Lifting global maintenance finally releases it.
        MAINTENANCE_MODE.remove_all();
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve once global maintenance is lifted")
            .unwrap();
    }
}
