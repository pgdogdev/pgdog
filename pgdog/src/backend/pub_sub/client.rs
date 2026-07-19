use crate::{
    backend::pub_sub::{channel_size, listener::Listener},
    net::NotificationResponse,
};

use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Notify, broadcast::error::RecvError, mpsc};
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
        let (tx, rx) = mpsc::channel(channel_size());

        Self {
            shutdown: Arc::new(Notify::new()),
            tx,
            rx,
            unlisten: HashMap::new(),
        }
    }

    /// Listen on a channel.
    pub fn listen(&mut self, channel: &str, mut rx: Listener) {
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
                                rx.stats().incr_recv();
                            },
                            Err(RecvError::Lagged(_)) => rx.stats().incr_dropped(),
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
    use std::time::Duration;

    use bytes::BufMut;
    use tokio::{task::yield_now, time::timeout};

    use crate::{
        backend::pub_sub::{StatsSnapshot, listener::test_support::TestChannel},
        net::{FromBytes, Payload},
    };

    use super::*;

    fn notification(channel: &str, payload: &str) -> NotificationResponse {
        let mut bytes = Payload::named('A');
        bytes.put_i32(1234);
        bytes.put_string(channel);
        bytes.put_string(payload);

        NotificationResponse::from_bytes(bytes.freeze()).expect("notification")
    }

    fn assert_snapshot(snapshot: StatsSnapshot, recv: u64, dropped: u64, listeners: u64) {
        assert_eq!(snapshot.recv, recv);
        assert_eq!(snapshot.dropped, dropped);
        assert_eq!(snapshot.listeners, listeners);
    }

    async fn recv_notification(client: &mut PubSubClient) -> NotificationResponse {
        timeout(Duration::from_secs(1), client.recv())
            .await
            .expect("timed out waiting for notification")
            .expect("notification")
    }

    async fn wait_for_listener_count(channel: &TestChannel, listeners: u64) {
        for _ in 0..10 {
            if channel.stats().listeners == listeners {
                return;
            }

            yield_now().await;
        }

        assert_eq!(channel.stats().listeners, listeners);
    }

    #[test]
    fn default_constructs_empty_client() {
        let client = PubSubClient::default();
        assert!(client.unlisten.is_empty());
    }

    #[tokio::test]
    async fn listen_forwards_notifications_to_client() {
        let channel = TestChannel::new();
        let mut client = PubSubClient::new();

        client.listen("events", channel.listener());
        channel
            .send(notification("events", "payload"))
            .expect("send notification");

        let message = recv_notification(&mut client).await;
        assert_eq!(message.channel(), "events");
        assert_eq!(message.payload(), "payload");
        assert_snapshot(channel.stats(), 1, 0, 1);
        assert_eq!(client.unlisten.len(), 1);
    }

    #[tokio::test]
    async fn unlisten_stops_forwarding_notifications() {
        let channel = TestChannel::new();
        let mut client = PubSubClient::new();

        client.listen("events", channel.listener());
        assert_eq!(client.unlisten.len(), 1);

        client.unlisten("events");
        assert!(client.unlisten.is_empty());
        wait_for_listener_count(&channel, 0).await;

        assert!(channel.send(notification("events", "payload")).is_err());
        assert!(
            timeout(Duration::from_millis(50), client.recv())
                .await
                .is_err()
        );
        assert_snapshot(channel.stats(), 0, 0, 0);
    }
}
