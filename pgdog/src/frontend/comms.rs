//! Communication to/from connected clients.

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::net::messages::BackendKeyData;

use super::{ConnectedClient, Stats};

static COMMS: Lazy<Comms> = Lazy::new(Comms::new);

/// Get global communication channel.
pub fn comms() -> Comms {
    COMMS.clone()
}

/// Sync primitives shared between all clients.
struct Global {
    shutdown: Notify,
    offline: AtomicBool,
    clients: Mutex<HashMap<BackendKeyData, ConnectedClient>>,
}

/// Bi-directional communications between client and internals.
#[derive(Clone)]
pub struct Comms {
    global: Arc<Global>,
    id: Option<BackendKeyData>,
}

impl Default for Comms {
    fn default() -> Self {
        Self::new()
    }
}

impl Comms {
    /// Create new communications channel between a client and pgDog.
    fn new() -> Self {
        Self {
            global: Arc::new(Global {
                shutdown: Notify::new(),
                offline: AtomicBool::new(false),
                clients: Mutex::new(HashMap::default()),
            }),
            id: None,
        }
    }

    /// Get all connected clients.
    pub fn clients(&self) -> HashMap<BackendKeyData, ConnectedClient> {
        self.global.clients.lock().clone()
    }

    /// New client connected.
    pub fn connect(&mut self, id: &BackendKeyData, addr: SocketAddr) -> Self {
        self.global
            .clients
            .lock()
            .insert(*id, ConnectedClient::new(addr));
        self.id = Some(*id);
        self.clone()
    }

    /// Client disconected.
    pub fn disconnect(&mut self) {
        if let Some(id) = self.id.take() {
            self.global.clients.lock().remove(&id);
        }
    }

    /// Update stats.
    pub fn stats(&self, stats: Stats) {
        if let Some(ref id) = self.id {
            let mut guard = self.global.clients.lock();
            if let Some(entry) = guard.get_mut(id) {
                entry.stats = stats;
            }
        }
    }

    /// Notify clients pgDog is shutting down.
    pub fn shutdown(&self) {
        self.global.offline.store(true, Ordering::Relaxed);
        self.global.shutdown.notify_waiters();
    }

    /// Wait for shutdown signal.
    pub async fn shutting_down(&self) {
        self.global.shutdown.notified().await
    }

    /// pgDog is shutting down now.
    pub fn offline(&self) -> bool {
        self.global.offline.load(Ordering::Relaxed)
    }
}
