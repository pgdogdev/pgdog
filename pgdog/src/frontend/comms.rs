//! Communication to/from connected clients.

use fnv::FnvHashMap as HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::net::messages::BackendKeyData;

use super::Stats;

struct Inner {
    shutdown: Notify,
    offline: AtomicBool,
    stats: Mutex<HashMap<BackendKeyData, Stats>>,
}

/// Bi-directional communications between client and internals.
#[derive(Clone)]
pub struct Comms {
    inner: Arc<Inner>,
    id: Option<BackendKeyData>,
}

impl Comms {
    /// Create new communications channel between a client and pgDog.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                shutdown: Notify::new(),
                offline: AtomicBool::new(false),
                stats: Mutex::new(HashMap::default()),
            }),
            id: None,
        }
    }

    /// New client connected.
    pub fn connect(&mut self, id: &BackendKeyData) -> Self {
        self.inner.stats.lock().insert(*id, Stats::new());
        self.id = Some(*id);
        self.clone()
    }

    /// Client disconected.
    pub fn disconnect(&mut self) {
        if let Some(id) = self.id.take() {
            self.inner.stats.lock().remove(&id);
        }
    }

    /// Update stats.
    pub fn stats(&self, stats: Stats) {
        if let Some(ref id) = self.id {
            self.inner.stats.lock().insert(*id, stats);
        }
    }

    /// Notify clients pgDog is shutting down.
    pub fn shutdown(&self) {
        self.inner.shutdown.notify_waiters();
        self.inner.offline.store(true, Ordering::Relaxed);
    }

    /// Wait for shutdown signal.
    pub async fn shutting_down(&self) {
        self.inner.shutdown.notified().await
    }

    /// pgDog is shutting down now.
    pub fn offline(&self) -> bool {
        self.inner.offline.load(Ordering::Relaxed)
    }
}
