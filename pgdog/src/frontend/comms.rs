//! Communication to/from connected clients.

use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio_util::task::TaskTracker;

use crate::net::messages::BackendKeyData;
use crate::net::Parameters;

use super::{ConnectedClient, Stats};

static COMMS: Lazy<Comms> = Lazy::new(Comms::new);

/// Get global communication channel.
pub fn comms() -> Comms {
    COMMS.clone()
}

/// Sync primitives shared between all clients.
struct Global {
    shutdown: Arc<Notify>,
    offline: AtomicBool,
    // This uses the FNV hasher, which is safe,
    // because BackendKeyData is randomly generated by us,
    // not by the client.
    clients: Mutex<HashMap<BackendKeyData, ConnectedClient>>,
    tracker: TaskTracker,
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
                shutdown: Arc::new(Notify::new()),
                offline: AtomicBool::new(false),
                clients: Mutex::new(HashMap::default()),
                tracker: TaskTracker::new(),
            }),
            id: None,
        }
    }

    /// Get all connected clients.
    pub fn clients(&self) -> HashMap<BackendKeyData, ConnectedClient> {
        self.global.clients.lock().clone()
    }

    /// Number of connected clients.
    pub fn clients_len(&self) -> usize {
        self.global.clients.lock().len()
    }

    pub fn tracker(&self) -> &TaskTracker {
        &self.global.tracker
    }

    /// Get number of connected clients.
    pub fn len(&self) -> usize {
        self.global.clients.lock().len()
    }

    /// There are no connected clients.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// New client connected.
    pub fn connect(&mut self, id: &BackendKeyData, addr: SocketAddr, params: &Parameters) -> Self {
        self.global
            .clients
            .lock()
            .insert(*id, ConnectedClient::new(addr, params));
        self.id = Some(*id);
        self.clone()
    }

    /// Client disconnected.
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
        self.global.tracker.close();
    }

    /// Wait for shutdown signal.
    pub fn shutting_down(&self) -> Arc<Notify> {
        self.global.shutdown.clone()
    }

    /// pgDog is shutting down now.
    pub fn offline(&self) -> bool {
        self.global.offline.load(Ordering::Relaxed)
    }
}
