#![allow(unused)] // Enterprise is the main consumer.
//! Failover signal listener.

use once_cell::sync::Lazy;
use tokio::sync::watch::*;

use crate::backend::pool::Address;

static WATCH: Lazy<Sender<Address>> = Lazy::new(|| {
    let (tx, _rx) = channel(Address::default());

    tx
});

pub(crate) struct FailoverSignal {
    rx: Receiver<Address>,
}

impl FailoverSignal {
    pub(crate) async fn recv(&mut self) -> Address {
        self.rx
            .changed()
            .await
            .expect("failover signal never closes");
        self.rx.borrow_and_update().clone()
    }
}

pub(super) fn notify(addr: &Address) {
    WATCH.send_replace(addr.clone());
}

pub(crate) fn watch() -> FailoverSignal {
    FailoverSignal {
        rx: WATCH.subscribe(),
    }
}
