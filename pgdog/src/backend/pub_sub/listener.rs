//! Pub/sub listener.
//!
//! Handles notifications from Postgres and sends them out
//! to a broadcast channel.
//!
use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use tokio::{
    select, spawn,
    sync::{broadcast, Notify},
};

use crate::{
    backend::{Error, Server},
    net::{FromBytes, Message, NotificationResponse, Protocol, ToBytes},
};

struct Inner {
    channels: Mutex<HashMap<String, broadcast::Sender<NotificationResponse>>>,
    subscription_requests: Mutex<Vec<String>>,
    subscription_notify: Notify,
    shutdown: Notify,
}

impl Inner {
    fn handle_message(&self, message: Message) -> Result<(), Error> {
        let notification = NotificationResponse::from_bytes(message.to_bytes()?)?;

        let guard = self.channels.lock();
        if let Some(channel) = guard.get(notification.channel()) {
            channel.send(notification).unwrap();
        }

        Ok(())
    }

    async fn subscribe(&self, server: &mut Server) -> Result<(), Error> {
        loop {
            let channel = self.subscription_requests.lock().pop();
            if let Some(channel) = channel {
                server.execute(format!("LISTEN {}", channel)).await?;
            } else {
                break;
            }
        }

        Ok(())
    }
}

/// Notification listener.
pub struct Listener {
    inner: Arc<Inner>,
}

impl Listener {
    pub async fn subscribe(
        &mut self,
        name: &str,
    ) -> Result<broadcast::Receiver<NotificationResponse>, Error> {
        let mut guard = self.inner.channels.lock();
        if let Some(channel) = guard.get(name) {
            Ok(channel.subscribe())
        } else {
            let (tx, rx) = broadcast::channel(4096);

            guard.insert(name.to_string(), tx);

            self.inner
                .subscription_requests
                .lock()
                .push(name.to_string());
            self.inner.subscription_notify.notify_one();

            Ok(rx)
        }
    }

    /// Create new listener on the server connection.
    pub fn new(mut server: Server) -> Result<Self, Error> {
        let inner = Arc::new(Inner {
            channels: Mutex::new(HashMap::new()),
            subscription_requests: Mutex::new(vec![]),
            subscription_notify: Notify::new(),
            shutdown: Notify::new(),
        });

        let task_inner = inner.clone();
        spawn(async move {
            loop {
                select! {
                    message = server.read() => {
                        let message = message?;

                        if message.code() != 'A' {
                            continue;
                        }

                        task_inner.handle_message(message)?;
                    }

                    _ = task_inner.subscription_notify.notified() => {
                        task_inner.subscribe(&mut server).await?;
                    }

                    _ = task_inner.shutdown.notified() => {
                        break;
                    }
                }
            }

            Ok::<(), Error>(())
        });

        Ok(Self { inner })
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.inner.shutdown.notify_one();
    }
}
