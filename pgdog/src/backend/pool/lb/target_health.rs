//! Keep a record of each pool's health.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug)]
struct Inner {
    healthy: AtomicBool,
    auth: AtomicBool,
}

#[derive(Clone, Debug)]
pub(crate) struct TargetHealth {
    inner: Arc<Inner>,
}

impl TargetHealth {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                healthy: AtomicBool::new(true),
                auth: AtomicBool::new(true),
            }),
        }
    }

    pub(crate) fn auth_ok(&self) -> bool {
        self.inner.auth.load(Ordering::Relaxed)
    }

    pub(crate) fn toggle_auth(&self, auth: bool) {
        self.inner.auth.store(auth, Ordering::SeqCst);
    }

    pub(crate) fn toggle_health(&self, healthy: bool) {
        self.inner.healthy.swap(healthy, Ordering::SeqCst);
    }

    pub(crate) fn healthy(&self) -> bool {
        self.inner.healthy.load(Ordering::Relaxed)
    }
}
