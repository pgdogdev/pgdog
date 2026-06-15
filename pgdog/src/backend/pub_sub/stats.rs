use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Stats {
    recv: AtomicU64,
    dropped: AtomicU64,
    listeners: AtomicU64,
}

#[derive(Debug, Default, Copy, Clone)]
pub struct StatsSnapshot {
    pub(crate) recv: u64,
    pub(crate) dropped: u64,
    pub(crate) listeners: u64,
}

impl Stats {
    pub(crate) fn incr_recv(&self) {
        self.recv.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn incr_listeners(&self) {
        self.listeners.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decr_listeners(&self) {
        self.listeners.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> StatsSnapshot {
        StatsSnapshot {
            recv: self.recv.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            listeners: self.listeners.load(Ordering::Relaxed),
        }
    }
}
