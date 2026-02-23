use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::Lazy;
use tokio::sync::{futures::Notified, Notify};
use tracing::warn;

static MAINTENANCE_MODE: Lazy<MaintenanceMode> = Lazy::new(|| MaintenanceMode {
    notify: Notify::new(),
    on: AtomicBool::new(false),
});

pub(crate) fn waiter() -> Option<Notified<'static>> {
    if !MAINTENANCE_MODE.on.load(Ordering::Relaxed) {
        None
    } else {
        let notified = MAINTENANCE_MODE.notify.notified();
        if !MAINTENANCE_MODE.on.load(Ordering::Relaxed) {
            None
        } else {
            Some(notified)
        }
    }
}

pub fn start() {
    MAINTENANCE_MODE.on.store(true, Ordering::Relaxed);
    warn!("maintenance mode is on");
}

pub fn stop() {
    MAINTENANCE_MODE.on.store(false, Ordering::Relaxed);
    MAINTENANCE_MODE.notify.notify_waiters();
    warn!("maintenance mode is off");
}

#[cfg(test)]
pub fn is_on() -> bool {
    MAINTENANCE_MODE.on.load(Ordering::Relaxed)
}

#[derive(Debug)]
struct MaintenanceMode {
    notify: Notify,
    on: AtomicBool,
}
