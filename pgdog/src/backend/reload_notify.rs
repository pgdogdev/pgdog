use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::Lazy;
use parking_lot::{Mutex, MutexGuard};
use tokio::sync::{futures::Notified, Notify};

static RELOAD_NOTIFY: Lazy<ReloadNotify> = Lazy::new(|| ReloadNotify {
    notify: Notify::new(),
    ready: AtomicBool::new(true),
    running: Mutex::new(()),
});

pub(crate) fn ready() -> Option<Notified<'static>> {
    if RELOAD_NOTIFY.ready.load(Ordering::Relaxed) {
        return None;
    }
    let notified = RELOAD_NOTIFY.notify.notified();
    if RELOAD_NOTIFY.ready.load(Ordering::Relaxed) {
        None
    } else {
        Some(notified)
    }
}

pub(super) fn started() -> MutexGuard<'static, ()> {
    let guard = RELOAD_NOTIFY.running.lock();
    RELOAD_NOTIFY.ready.store(false, Ordering::Relaxed);

    guard
}

pub(super) fn done() {
    RELOAD_NOTIFY.ready.store(true, Ordering::Relaxed);
    RELOAD_NOTIFY.notify.notify_waiters();
}

#[derive(Debug)]
struct ReloadNotify {
    notify: Notify,
    ready: AtomicBool,
    running: Mutex<()>,
}
