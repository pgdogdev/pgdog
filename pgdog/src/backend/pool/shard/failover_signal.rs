#![allow(unused)] // Enterprise is the main consumer.
//! Failover signal listener.

use once_cell::sync::Lazy;
use tokio::sync::watch::*;

#[derive(Debug, Clone)]
pub(super) struct FailoverSignal {
    tx: Sender<()>,
}

impl Default for FailoverSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl FailoverSignal {
    pub(super) fn new() -> Self {
        let (tx, _) = channel(());
        Self { tx }
    }

    pub(super) fn watch(&self) -> FailoverSignalWatcher {
        FailoverSignalWatcher {
            rx: self.tx.subscribe(),
        }
    }

    pub(super) fn notify(&self) {
        self.tx.send_replace(());
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FailoverSignalWatcher {
    rx: Receiver<()>,
}

impl FailoverSignalWatcher {
    pub(crate) async fn recv(&mut self) {
        self.rx
            .changed()
            .await
            .expect("failover signal never closes");
        self.rx.borrow_and_update();
    }
}
