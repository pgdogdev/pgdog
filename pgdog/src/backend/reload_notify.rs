use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::Lazy;
use tokio::sync::{futures::Notified, Notify};

static RELOAD_NOTIFY: Lazy<ReloadNotify> = Lazy::new(|| ReloadNotify {
    notify: Notify::new(),
    ready: AtomicBool::new(true),
});

pub(crate) fn ready() -> Option<Notified<'static>> {
    let notified = RELOAD_NOTIFY.notify.notified();
    if RELOAD_NOTIFY.ready.load(Ordering::Relaxed) {
        None
    } else {
        Some(notified)
    }
}

pub(super) fn started() {
    RELOAD_NOTIFY.ready.store(false, Ordering::Relaxed);
}

pub(super) fn done() {
    RELOAD_NOTIFY.ready.store(true, Ordering::Relaxed);
    RELOAD_NOTIFY.notify.notify_waiters();
}

#[derive(Debug)]
struct ReloadNotify {
    notify: Notify,
    ready: AtomicBool,
}
