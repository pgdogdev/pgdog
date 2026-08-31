#![allow(unused)] // Enterprise consumes this.

use once_cell::sync::Lazy;
use pgdog_config::ConfigAndUsers;
use std::sync::Arc;
use tokio::sync::watch::*;

static WATCHER: Lazy<Sender<Arc<ConfigAndUsers>>> = Lazy::new(|| {
    let (tx, _rx) = channel(Arc::new(ConfigAndUsers::default()));

    tx
});

pub(crate) struct ConfigWatcher {
    rx: Receiver<Arc<ConfigAndUsers>>,
}

impl ConfigWatcher {
    pub(crate) async fn recv(&mut self) -> Arc<ConfigAndUsers> {
        self.rx
            .changed()
            .await
            .expect("config watcher never closes");

        let val = self.rx.borrow_and_update().clone();

        val
    }
}

pub(super) fn notify(config: Arc<ConfigAndUsers>) {
    WATCHER.send_replace(config);
}

pub(crate) fn watch() -> ConfigWatcher {
    ConfigWatcher {
        rx: WATCHER.subscribe(),
    }
}
