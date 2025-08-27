use crate::config::config;
use crate::net::NotificationResponse;

use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc, Notify,
};
use tokio::{select, spawn};

#[derive(Debug)]
pub struct PubSubClient {
    shutdown: Arc<Notify>,
    tx: mpsc::Sender<NotificationResponse>,
    rx: mpsc::Receiver<NotificationResponse>,
    unlisten: HashMap<String, Arc<Notify>>,
}

impl Default for PubSubClient {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubClient {
    pub fn new() -> Self {
        let size = config().config.general.pub_sub_channel_size;
        let (tx, rx) = mpsc::channel(std::cmp::max(1, size));

        Self {
            shutdown: Arc::new(Notify::new()),
            tx,
            rx,
            unlisten: HashMap::new(),
        }
    }

    /// Listen on a channel.
    pub fn listen(&mut self, channel: &str, mut rx: broadcast::Receiver<NotificationResponse>) {
        let shutdown = self.shutdown.clone();
        let tx = self.tx.clone();

        let unlisten = Arc::new(Notify::new());
        self.unlisten.insert(channel.to_string(), unlisten.clone());

        spawn(async move {
            loop {
                select! {
                    _ = shutdown.notified() => {
                        return;
                    }

                    _ = unlisten.notified() => {
                        return;
                    }

                    message = rx.recv() => {
                        match message {
                            Ok(message) => {
                                if tx.send(message).await.is_err() {
                                    return;
                                }
                            },
                            Err(RecvError::Lagged(_)) => (),
                            Err(RecvError::Closed) => return,
                        }
                    }
                }
            }
        });
    }

    /// Wait for a message from the pub/sub channel.
    pub async fn recv(&mut self) -> Option<NotificationResponse> {
        self.rx.recv().await
    }

    /// Stop listening on a channel.
    pub fn unlisten(&mut self, channel: &str) {
        if let Some(notify) = self.unlisten.remove(channel) {
            notify.notify_one();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_empty_pub_sub_client() {
        let _client = PubSubClient::new();
    }
}
