use crate::config::config;
use crate::net::NotificationResponse;

use std::sync::Arc;
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
}

impl Default for PubSubClient {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubClient {
    pub fn new() -> Self {
        let size = config().config.general.pub_sub_channel_size;
        let (tx, rx) = mpsc::channel(std::cmp::min(1, size));

        Self {
            shutdown: Arc::new(Notify::new()),
            tx,
            rx,
        }
    }

    pub fn listen(&self, mut rx: broadcast::Receiver<NotificationResponse>) {
        let shutdown = self.shutdown.clone();
        let tx = self.tx.clone();

        spawn(async move {
            loop {
                select! {
                    _ = shutdown.notified() => {
                        return;
                    }

                    message = rx.recv() => {
                        match message {
                            Ok(message) => {
                                if let Err(_) = tx.send(message).await {
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
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_empty_pub_sub_client() {
        let _client = PubSubClient::new();
    }
}
