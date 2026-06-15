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
    select, spawn,
    sync::{Notify, broadcast, mpsc},
    time::sleep,
};
use tracing::{debug, error, info};

use super::{Stats, StatsSnapshot};
use crate::{
    backend::{self, ConnectReason, DisconnectReason, Pool, pool::Error},
    config::config,
    net::{
        FromBytes, FrontendPid, NotificationResponse, Parameter, Parameters, Protocol,
        ProtocolMessage, Query, ToBytes,
    },
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

type Channels = Arc<Mutex<HashMap<String, Channel>>>;

static CHANNELS: Lazy<Channels> = Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Get stats for all channels.
pub fn stats() -> HashMap<String, StatsSnapshot> {
    CHANNELS
        .lock()
        .iter()
        .map(|(name, channel)| (name.to_string(), channel.stats.get()))
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

#[derive(Debug)]
struct Comms {
    start: Notify,
    shutdown: Notify,
}

/// Notification listener.
#[derive(Debug, Clone)]
pub struct PubSubListener {
    id: FrontendPid,
    pool: Pool,
    tx: mpsc::Sender<Request>,
    channels: Channels,
    comms: Arc<Comms>,
}

impl PubSubListener {
    /// Create new listener on the server connection.
    pub fn new(pool: &Pool) -> Self {
        let (tx, mut rx) = mpsc::channel(config().config.general.pub_sub_channel_size);

        let pool = pool.clone();
        let channels = CHANNELS.clone();

        let listener = Self {
            id: FrontendPid::new(),
            pool: pool.clone(),
            tx,
            channels,
            comms: Arc::new(Comms {
                start: Notify::new(),
                shutdown: Notify::new(),
            }),
        };

        let id = listener.id;
        let channels = listener.channels.clone();
        let pool = listener.pool.clone();
        let comms = listener.comms.clone();
        spawn(async move {
            loop {
                comms.start.notified().await;

                select! {
                    _ = comms.shutdown.notified() => {
                        rx.close(); // Drain remaining messages.
                    }

                    result = Self::run(id, &pool, &mut rx, channels.clone()) => {
                        if let Err(err) = result {
                            error!("pub/sub error: {} [{}]", err, pool.addr());
                            // Don't reconnect for another connect attempt delay
                            // to avoid connection storms during incidents.
                            sleep(Duration::from_millis(config().config.general.connect_attempt_delay)).await;
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
        self.comms.shutdown.notify_one();
    }

    /// Listen on a channel.
    pub async fn listen(&self, channel_name: &str) -> Result<Listener, Error> {
        let listener = {
            let mut guard = self.channels.lock();

            if let Some(channel) = guard.get(channel_name) {
                return Ok(Listener::new(channel));
            }

            let (tx, _) = broadcast::channel(config().config.general.pub_sub_channel_size);
            let stats = Arc::new(Stats::default());

            let channel = Channel {
                tx,
                stats: stats.clone(),
            };
            let listener = Listener::new(&channel);

            guard.insert(channel_name.to_string(), channel);

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

        // Re-listen on all channels when re-starting the task.
        // We don't lose LISTEN commands.
        let resub = channels
            .lock()
            .keys()
            .map(|channel| Request::Subscribe(channel.to_string()).into())
            .collect::<Vec<ProtocolMessage>>();

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
                        if let Some(channel) = channels.lock().get(notification.channel()) {
                            match channel.tx.send(notification) {
                                Ok(_) => (),
                                Err(err) => unsub = Some(err.0.channel().to_string()),
                            }
                        }

                        if let Some(unsub) = unsub {
                            channels.lock().remove(&unsub);
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
