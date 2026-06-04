//! Communication to/from connected clients.

use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use fnv::FnvHashMap as HashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio_util::task::TaskTracker;

use crate::net::messages::{BackendKeyData, FrontendPid};
use crate::net::Parameters;

use super::{ConnectedClient, Stats};

static COMMS: Lazy<Comms> = Lazy::new(Comms::new);

/// Get global communication channel.
pub fn comms() -> Comms {
    COMMS.clone()
}

/// Sync primitives shared between all clients.
#[derive(Debug)]
struct Global {
    shutdown: Arc<Notify>,
    offline: AtomicBool,
    // This uses the FNV hasher, which is safe,
    // because FrontendPid is monotonically minted by us,
    // not derived from untrusted client input.
    clients: Mutex<HashMap<FrontendPid, ConnectedClient>>,
    tracker: TaskTracker,
}

/// Bi-directional communications between client and internals.
#[derive(Clone, Debug)]
pub struct Comms {
    global: Arc<Global>,
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
        }
    }

    /// Get all connected clients.
    pub fn clients(&self) -> HashMap<FrontendPid, ConnectedClient> {
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

    /// New client connected.
    pub fn connect(&self, key: BackendKeyData, addr: SocketAddr, params: &Parameters) {
        let pid = FrontendPid::from(&key);
        self.global
            .clients
            .lock()
            .insert(pid, ConnectedClient::new(key, addr, params));
    }

    /// Update client parameters.
    pub fn update_params(&self, id: FrontendPid, params: Parameters) {
        let mut guard = self.global.clients.lock();
        if let Some(entry) = guard.get_mut(&id) {
            entry.paramters = params;
        }
    }

    /// Client disconnected.
    pub fn disconnect(&self, id: FrontendPid) {
        self.global.clients.lock().remove(&id);
    }

    /// Update stats.
    pub fn update_stats(&self, id: FrontendPid, stats: Stats) {
        let mut guard = self.global.clients.lock();
        if let Some(entry) = guard.get_mut(&id) {
            entry.stats = stats;
        }
    }

    /// Verify that a cancel request has a valid secret for the given client.
    pub fn verify_cancel(&self, key: &BackendKeyData) -> bool {
        let pid = FrontendPid::from(key);
        self.global
            .clients
            .lock()
            .get(&pid)
            .map(|client| client.key.secret.constant_time_eq(&key.secret))
            .unwrap_or(false)
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

#[derive(Debug, Clone)]
pub struct ClientComms {
    comms: Comms,
    id: FrontendPid,
}

impl Deref for ClientComms {
    type Target = Comms;

    fn deref(&self) -> &Self::Target {
        &self.comms
    }
}

impl ClientComms {
    pub fn disconnect(&self) {
        self.comms.disconnect(self.id);
    }

    pub fn update_stats(&self, stats: Stats) {
        self.comms.update_stats(self.id, stats);
    }

    pub fn new(id: FrontendPid) -> Self {
        Self { id, comms: comms() }
    }

    pub fn connect(&self, key: BackendKeyData, addr: SocketAddr, params: &Parameters) {
        self.comms.connect(key, addr, params)
    }

    pub fn update_params(&self, params: &Parameters) {
        self.comms.update_params(self.id, params.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::net::{
        messages::{BackendKeyData, ProtocolVersion},
        Parameters,
    };

    fn addr() -> SocketAddr {
        "127.0.0.1:5432".parse().unwrap()
    }

    #[test]
    fn test_verify_cancel_correct_secret() {
        let comms = Comms::default();
        let key = BackendKeyData::new_frontend(ProtocolVersion::V3_0, FrontendPid::new());
        comms.connect(key.clone(), addr(), &Parameters::default());
        assert!(comms.verify_cancel(&key));
    }

    #[test]
    fn test_verify_cancel_wrong_secret() {
        let comms = Comms::default();
        let key = BackendKeyData::new_frontend(ProtocolVersion::V3_0, FrontendPid::new());
        comms.connect(key.clone(), addr(), &Parameters::default());

        // Same pid, different secret.
        let wrong = BackendKeyData::legacy(key.pid(), 0);
        assert!(!comms.verify_cancel(&wrong));
    }

    #[test]
    fn test_verify_cancel_unknown_pid() {
        let comms = Comms::default();
        // Nothing registered — any key must be rejected.
        assert!(!comms.verify_cancel(&BackendKeyData::new_frontend(
            ProtocolVersion::V3_0,
            FrontendPid::new()
        )));
    }

    #[test]
    fn test_verify_cancel_after_disconnect() {
        let comms = Comms::default();
        let id = FrontendPid::new();
        let key = BackendKeyData::new_frontend(ProtocolVersion::V3_0, id);
        comms.connect(key.clone(), addr(), &Parameters::default());
        assert!(comms.verify_cancel(&key));

        comms.disconnect(id);
        assert!(!comms.verify_cancel(&key));
    }
}
