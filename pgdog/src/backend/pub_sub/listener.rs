//! Pub/sub listener.
//!
//! Handles notifications from Postgres and sends them out
//! to a broadcast channel.
//!
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use tokio::{
    select, spawn,
    sync::{broadcast, Notify},
};
use tracing::{error, info};

use crate::{
    backend::{pool::Address, Error, Server, ServerOptions},
    config::config,
    net::{FromBytes, Message, NotificationResponse, Protocol, ToBytes},
};

use super::inner::Inner;

/// Notification listener.
#[derive(Debug, Clone)]
pub struct Listener {
    inner: Arc<Inner>,
}

impl Listener {
    pub async fn subscribe(
        &mut self,
        name: &str,
    ) -> Result<broadcast::Receiver<NotificationResponse>, Error> {
        self.inner.request_subscribe(name)
    }

    /// Create new listener on the server connection.
    pub fn new(addr: &Address) -> Self {
        let inner = Arc::new(Inner::new());
        let addr = addr.clone();

        let task_inner = inner.clone();

        spawn(async move {
            loop {
                select! {
                    _ = task_inner.shutdown.notified() => { break; },
                    _ = task_inner.restart.notified() => {
                        let task_inner = task_inner.clone();
                        let addr = addr.clone();

                        spawn(async move {
                            if let Err(err) = task_inner.serve(&addr).await {
                                error!("listener error: {} [{}]", err, addr);
                                task_inner.restart.notify_one();
                            }
                        });
                    }
                }
            }
        });

        Self { inner }
    }

    pub fn start(&self) {
        self.inner.restart.notify_one();
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.inner.shutdown.notify_waiters();
    }
}
