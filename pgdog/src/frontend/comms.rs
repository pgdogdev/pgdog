//! Communication to/from connected clients.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::Notify;

struct Inner {
    shutdown: Notify,
    offline: AtomicBool,
}

/// Bi-directional communications between client and internals.
#[derive(Clone)]
pub struct Comms {
    inner: Arc<Inner>,
}

impl Comms {
    /// Create new communications channel between a client and pgDog.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                shutdown: Notify::new(),
                offline: AtomicBool::new(false),
            }),
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
