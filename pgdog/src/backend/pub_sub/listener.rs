//! Pub/sub listener.
//!
//! Handles notifications from Postgres and sends them out
//! to a broadcast channel.
//!
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::{
    select,
    sync::{Notify, broadcast, mpsc},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use super::{Stats, StatsSnapshot, channel_size};
use crate::{
    backend::{self, ConnectReason, DisconnectReason, Pool, databases::User, pool::Error},
    config::config,
    net::{
        FromBytes, FrontendPid, NotificationResponse, Parameter, Parameters, Protocol,
        ProtocolMessage, Query, ToBytes,
    },
    tasks,
};

#[derive(Debug, Clone)]
enum Request {
    Unsubscribe(String),
    Subscribe(String),
    Notify { channel: String, payload: String },
}

impl From<Request> for ProtocolMessage {
    fn from(val: Request) -> Self {
        match val {
            Request::Unsubscribe(channel) => Query::new(format!("UNLISTEN \"{}\"", channel)).into(),
            Request::Subscribe(channel) => Query::new(format!("LISTEN \"{}\"", channel)).into(),
            Request::Notify { channel, payload } => {
                Query::new(format!("NOTIFY \"{}\", '{}'", channel, payload)).into()
            }
        }
    }
}

/// Pool a set of channels belongs to. `NOTIFY` is scoped to a database, so
/// channels have to be scoped the same way.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PoolKey {
    /// Database name, as configured in pgdog.
    pub database: String,
    /// User, as configured in pgdog.
    pub user: String,
    /// Shard number.
    pub shard: usize,
}

impl PoolKey {
    fn new(identifier: &User, shard: usize) -> Self {
        Self {
            database: identifier.database.clone(),
            user: identifier.user.clone(),
            shard,
        }
    }

    /// Key for one of this pool's channels.
    fn channel(&self, channel: &str) -> ChannelKey {
        ChannelKey {
            pool: self.clone(),
            channel: channel.to_owned(),
        }
    }
}

/// One channel on one pool.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChannelKey {
    /// Pool the channel belongs to.
    pub pool: PoolKey,
    /// Channel name used by `LISTEN`/`NOTIFY`.
    pub channel: String,
}

type Channels = Arc<Mutex<HashMap<PoolKey, HashMap<String, Channel>>>>;

static CHANNELS: Lazy<Channels> = Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Get stats for all channels.
pub fn stats() -> HashMap<ChannelKey, StatsSnapshot> {
    CHANNELS
        .lock()
        .iter()
        .flat_map(|(pool, channels)| {
            channels
                .iter()
                .map(|(channel, state)| (pool.channel(channel), state.stats.get()))
        })
        .collect()
}

#[derive(Debug)]
struct Channel {
    tx: broadcast::Sender<NotificationResponse>,
    stats: Arc<Stats>,
}

#[derive(Debug)]
pub struct Listener {
    rx: broadcast::Receiver<NotificationResponse>,
    stats: Arc<Stats>,
}

impl Listener {
    fn new(channel: &Channel) -> Self {
        channel.stats.incr_listeners();

        Self {
            rx: channel.tx.subscribe(),
            stats: channel.stats.clone(),
        }
    }

    pub(crate) fn stats(&self) -> &Stats {
        &self.stats
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.stats.decr_listeners();
    }
}

impl Deref for Listener {
    type Target = broadcast::Receiver<NotificationResponse>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for Listener {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) struct TestChannel {
        tx: broadcast::Sender<NotificationResponse>,
        stats: Arc<Stats>,
    }

    impl TestChannel {
        pub(crate) fn new() -> Self {
            let (tx, _) = broadcast::channel(4);

            Self {
                tx,
                stats: Arc::new(Stats::default()),
            }
        }

        pub(crate) fn listener(&self) -> Listener {
            Listener::new(&Channel {
                tx: self.tx.clone(),
                stats: self.stats.clone(),
            })
        }

        pub(crate) fn send(
            &self,
            notification: NotificationResponse,
        ) -> Result<usize, broadcast::error::SendError<NotificationResponse>> {
            self.tx.send(notification)
        }

        pub(crate) fn stats(&self) -> StatsSnapshot {
            self.stats.get()
        }
    }
}

#[derive(Debug)]
struct Comms {
    start: Notify,
    shutdown: CancellationToken,
}

/// Notification listener.
#[derive(Debug, Clone)]
pub struct PubSubListener {
    id: FrontendPid,
    pool: Pool,
    pool_key: PoolKey,
    tx: mpsc::Sender<Request>,
    channels: Channels,
    comms: Arc<Comms>,
}

impl PubSubListener {
    /// Create new listener on the server connection.
    ///
    /// `identifier` and `shard` scope the channels this listener owns. They are
    /// the pgdog-side identity of the pool, so they survive a primary being
    /// promoted underneath us, which the server address would not.
    pub fn new(pool: &Pool, identifier: &User, shard: usize) -> Self {
        let (tx, mut rx) = mpsc::channel(channel_size());

        let pool = pool.clone();
        let channels = CHANNELS.clone();

        let listener = Self {
            id: FrontendPid::new(),
            pool: pool.clone(),
            pool_key: PoolKey::new(identifier, shard),
            tx,
            channels,
            comms: Arc::new(Comms {
                start: Notify::new(),
                shutdown: CancellationToken::new(),
            }),
        };

        let id = listener.id;
        let channels = listener.channels.clone();
        let pool_key = listener.pool_key.clone();
        let pool = listener.pool.clone();
        let comms = listener.comms.clone();
        tasks::spawn("pub sub", async move {
            loop {
                select! {
                    _ = comms.start.notified() => {}
                    _ = comms.shutdown.cancelled() => {
                        rx.close();
                    }
                }

                if rx.is_closed() {
                    break;
                }

                select! {
                    _ = comms.shutdown.cancelled() => {
                        rx.close(); // Drain remaining messages.
                    }

                    result = Self::run(id, &pool, &pool_key, &mut rx, channels.clone()) => {
                        if let Err(err) = result {
                            error!("pub/sub error: {} [{}]", err, pool.addr());
                            // Don't reconnect for another connect attempt delay
                            // to avoid connection storms during incidents.
                            select! {
                                _ = sleep(Duration::from_millis(config().config.general.connect_attempt_delay)) => {}
                                _ = comms.shutdown.cancelled() => rx.close(),
                            }
                        }
                    }
                }

                if rx.is_closed() {
                    break;
                }
            }
        });

        listener
    }

    /// Launch the listener.
    pub fn launch(&self) {
        self.comms.start.notify_one();
    }

    /// Shutdown the listener.
    pub fn shutdown(&self) {
        self.comms.shutdown.cancel();
    }

    /// Listen on a channel.
    pub async fn listen(&self, channel_name: &str) -> Result<Listener, Error> {
        let listener = {
            let mut guard = self.channels.lock();
            let channels = guard.entry(self.pool_key.clone()).or_default();

            if let Some(channel) = channels.get(channel_name) {
                return Ok(Listener::new(channel));
            }

            let (tx, _) = broadcast::channel(channel_size());
            let stats = Arc::new(Stats::default());

            let channel = Channel {
                tx,
                stats: stats.clone(),
            };
            let listener = Listener::new(&channel);

            channels.insert(channel_name.to_string(), channel);

            listener
        };

        self.tx
            .send(Request::Subscribe(channel_name.to_string()))
            .await
            .map_err(|_| Error::Offline)?;

        Ok(listener)
    }

    /// Notify a channel with payload.
    pub async fn notify(&self, channel: &str, payload: &str) -> Result<(), Error> {
        self.tx
            .send(Request::Notify {
                channel: channel.to_string(),
                payload: payload.to_string(),
            })
            .await
            .map_err(|_| Error::Offline)
    }

    // Run the listener task.
    async fn run(
        id: FrontendPid,
        pool: &Pool,
        pool_key: &PoolKey,
        rx: &mut mpsc::Receiver<Request>,
        channels: Channels,
    ) -> Result<(), backend::Error> {
        info!("pub/sub started [{}]", pool.addr());

        let mut server = pool.standalone(ConnectReason::PubSub).await?;

        server
            .link_client(
                id,
                &Parameters::from(vec![Parameter {
                    name: "application_name".into(),
                    value: "PgDog Pub/Sub Listener".into(),
                }]),
                None,
            )
            .await?;

        // Re-listen on this pool's channels when re-starting the task.
        // We don't lose LISTEN commands.
        let resub = channels
            .lock()
            .get(pool_key)
            .map(|channels| {
                channels
                    .keys()
                    .map(|channel| Request::Subscribe(channel.clone()).into())
                    .collect::<Vec<ProtocolMessage>>()
            })
            .unwrap_or_default();

        if !resub.is_empty() {
            server.send(&resub.into()).await?;
        }

        loop {
            select! {
                message = server.read() => {
                    let message = message?;

                    // NotificationResponse (B)
                    if message.code() == 'A' {
                        let notification = NotificationResponse::from_bytes(message.to_bytes())?;
                        let mut unsub = None;
                        if let Some(channel) = channels
                            .lock()
                            .get(pool_key)
                            .and_then(|channels| channels.get(notification.channel()))
                        {
                            match channel.tx.send(notification) {
                                Ok(_) => (),
                                Err(err) => unsub = Some(err.0.channel().to_string()),
                            }
                        }

                        if let Some(unsub) = unsub {
                            if let Some(channels) = channels.lock().get_mut(pool_key) {
                                channels.remove(&unsub);
                            }
                            server.send(&vec![Request::Unsubscribe(unsub).into()].into()).await?;
                        }
                    }

                    // Terminate (B)
                    if message.code() == 'X' {
                        break;
                    }
                }

                req = rx.recv() => {
                    if let Some(req) = req {
                        debug!("pub/sub request {:?}", req);
                        server.send(&vec![req.into()].into()).await?;
                    } else {
                        server.disconnect_reason(DisconnectReason::Offline);
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use parking_lot::Mutex;
    use tokio::sync::{Notify, mpsc};

    use super::{test_support::TestChannel, *};

    fn test_user(user: &str, database: &str) -> User {
        User {
            user: user.into(),
            database: database.into(),
        }
    }

    fn test_pub_sub_listener() -> (PubSubListener, mpsc::Receiver<Request>) {
        test_pub_sub_listener_on(
            Arc::new(Mutex::new(HashMap::new())),
            &test_user("pgdog", "pgdog"),
            0,
        )
    }

    /// A listener for `identifier`/`shard`, sharing `channels` with any other
    /// listener built from the same map.
    fn test_pub_sub_listener_on(
        channels: Channels,
        identifier: &User,
        shard: usize,
    ) -> (PubSubListener, mpsc::Receiver<Request>) {
        let (tx, rx) = mpsc::channel(4);

        (
            PubSubListener {
                id: FrontendPid::new(),
                pool: Pool::new_test(),
                pool_key: PoolKey::new(identifier, shard),
                tx,
                channels,
                comms: Arc::new(Comms {
                    start: Notify::new(),
                    shutdown: CancellationToken::new(),
                }),
            },
            rx,
        )
    }

    /// Assert a Subscribe request is already queued. Deliberately non-blocking:
    /// the sender is alive, so an `await` here would hang rather than fail if a
    /// regression stopped the request being sent.
    fn expect_subscribe_now(rx: &mut mpsc::Receiver<Request>, expected: &str) {
        match rx.try_recv() {
            Ok(Request::Subscribe(channel)) => assert_eq!(channel, expected),
            other => panic!("expected subscribe request for {expected}, got {other:?}"),
        }
    }

    fn assert_snapshot(snapshot: StatsSnapshot, recv: u64, dropped: u64, listeners: u64) {
        assert_eq!(snapshot.recv, recv);
        assert_eq!(snapshot.dropped, dropped);
        assert_eq!(snapshot.listeners, listeners);
    }

    fn assert_request_query(request: Request, expected: &str) {
        let ProtocolMessage::Query(query) = ProtocolMessage::from(request) else {
            panic!("request should convert to a query message");
        };

        assert_eq!(query.query(), expected);
    }

    async fn expect_subscribe(rx: &mut mpsc::Receiver<Request>, expected: &str) {
        let request = rx.recv().await.expect("request");

        match request {
            Request::Subscribe(channel) => assert_eq!(channel, expected),
            request => panic!("expected subscribe request, got {request:?}"),
        }
    }

    async fn expect_notify(
        rx: &mut mpsc::Receiver<Request>,
        expected_channel: &str,
        expected_payload: &str,
    ) {
        let request = rx.recv().await.expect("request");

        match request {
            Request::Notify { channel, payload } => {
                assert_eq!(channel, expected_channel);
                assert_eq!(payload, expected_payload);
            }
            request => panic!("expected notify request, got {request:?}"),
        }
    }

    #[test]
    fn requests_convert_to_expected_sql_queries() {
        assert_request_query(Request::Subscribe("events".into()), "LISTEN \"events\"");
        assert_request_query(Request::Unsubscribe("events".into()), "UNLISTEN \"events\"");
        assert_request_query(
            Request::Notify {
                channel: "events".into(),
                payload: "payload".into(),
            },
            "NOTIFY \"events\", 'payload'",
        );
    }

    #[test]
    fn listener_drop_updates_listener_count() {
        let channel = TestChannel::new();
        assert_snapshot(channel.stats(), 0, 0, 0);

        let first = channel.listener();
        assert_snapshot(channel.stats(), 0, 0, 1);

        {
            let _second = channel.listener();
            assert_snapshot(channel.stats(), 0, 0, 2);
        }

        assert_snapshot(channel.stats(), 0, 0, 1);
        drop(first);
        assert_snapshot(channel.stats(), 0, 0, 0);
    }

    #[tokio::test]
    async fn listen_creates_channel_once_and_reuses_it() {
        let (pub_sub, mut rx) = test_pub_sub_listener();

        let first = pub_sub.listen("events").await.expect("first listen");
        expect_subscribe(&mut rx, "events").await;
        assert_eq!(pub_sub.channels.lock().len(), 1);
        assert_snapshot(first.stats().get(), 0, 0, 1);

        let second = pub_sub.listen("events").await.expect("second listen");
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(pub_sub.channels.lock().len(), 1);
        assert_snapshot(second.stats().get(), 0, 0, 2);

        drop(first);
        assert_snapshot(second.stats().get(), 0, 0, 1);

        drop(second);
        let stats = pub_sub
            .channels
            .lock()
            .get(&pub_sub.pool_key)
            .and_then(|channels| channels.get("events"))
            .expect("events channel")
            .stats
            .get();
        assert_snapshot(stats, 0, 0, 0);
    }

    /// Two listeners sharing the registry must not share a channel just because
    /// they were handed the same name. Each has to be sent its own LISTEN,
    /// otherwise the one that misses out never receives its own database's
    /// notifications.
    async fn assert_channels_not_shared(
        left: &User,
        left_shard: usize,
        right: &User,
        right_shard: usize,
    ) {
        let channels: Channels = Arc::new(Mutex::new(HashMap::new()));
        let (first, mut first_rx) = test_pub_sub_listener_on(channels.clone(), left, left_shard);
        let (second, mut second_rx) =
            test_pub_sub_listener_on(channels.clone(), right, right_shard);

        let _first = first.listen("events").await.expect("first listen");
        let _second = second.listen("events").await.expect("second listen");

        expect_subscribe_now(&mut first_rx, "events");
        expect_subscribe_now(&mut second_rx, "events");

        let guard = channels.lock();
        assert_eq!(guard.len(), 2, "each pool needs its own channel set");
        for pool_key in [&first.pool_key, &second.pool_key] {
            let channel = guard
                .get(pool_key)
                .and_then(|channels| channels.get("events"))
                .expect("channel for pool");
            assert_snapshot(channel.stats.get(), 0, 0, 1);
        }
    }

    #[tokio::test]
    async fn channels_are_not_shared_between_databases() {
        assert_channels_not_shared(
            &test_user("pgdog", "first_database"),
            0,
            &test_user("pgdog", "second_database"),
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn channels_are_not_shared_between_users() {
        assert_channels_not_shared(
            &test_user("alice", "pgdog"),
            0,
            &test_user("bob", "pgdog"),
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn channels_are_not_shared_between_shards() {
        let user = test_user("pgdog", "pgdog");
        assert_channels_not_shared(&user, 0, &user, 1).await;
    }

    #[test]
    fn pool_key_is_built_from_the_pgdog_side_identity() {
        let key = PoolKey::new(&test_user("alice", "shop"), 2);

        assert_eq!(key.database, "shop");
        assert_eq!(key.user, "alice");
        assert_eq!(key.shard, 2);

        // Every component discriminates.
        assert_ne!(key, PoolKey::new(&test_user("alice", "other"), 2));
        assert_ne!(key, PoolKey::new(&test_user("bob", "shop"), 2));
        assert_ne!(key, PoolKey::new(&test_user("alice", "shop"), 3));
    }

    #[tokio::test]
    async fn notify_queues_notify_request() {
        let (pub_sub, mut rx) = test_pub_sub_listener();

        pub_sub
            .notify("events", "payload")
            .await
            .expect("notify request");

        expect_notify(&mut rx, "events", "payload").await;
    }
}
