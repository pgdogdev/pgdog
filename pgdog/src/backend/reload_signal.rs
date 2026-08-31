#![allow(unused)]
//! Notify anyone who's listening about config reloads.

use once_cell::sync::Lazy;
use tokio::sync::watch::*;

static WATCH: Lazy<Sender<()>> = Lazy::new(|| {
    let (tx, _) = channel(());
    tx
});

pub(crate) struct DatabaseWatcher {
    rx: Receiver<()>,
}

impl DatabaseWatcher {
    pub(crate) async fn changed(&mut self) {
        self.rx
            .changed()
            .await
            .expect("database reload signal never closes");
        self.rx.borrow_and_update();
    }
}

pub(crate) fn watch() -> DatabaseWatcher {
    DatabaseWatcher {
        rx: WATCH.subscribe(),
    }
}

pub(super) fn notify() {
    WATCH.send_replace(());
}
