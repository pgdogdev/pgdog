//! Pub/sub listener.
//!
//! Handles notifications from Postgres and sends them out
//! to a broadcast channel.
//!
use std::{collections::HashMap, sync::Arc, time::Duration};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::{
    select, spawn,
    sync::{broadcast, mpsc, Notify},
    time::sleep,
};
use tracing::{debug, error, info};

use crate::{
    backend::{self, pool::Error, Pool},
    config::config,
    net::{
        FromBytes, NotificationResponse, Parameter, Parameters, Protocol, ProtocolMessage, Query,
        ToBytes,
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

type Channels = Arc<Mutex<HashMap<String, broadcast::Sender<NotificationResponse>>>>;

static CHANNELS: Lazy<Channels> = Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

#[derive(Debug)]
struct Comms {
    start: Notify,
    shutdown: Notify,
}

/// Notification listener.
#[derive(Debug, Clone)]
pub(crate) struct PubSubListener {
    pool: Pool,
    tx: mpsc::Sender<Request>,
    channels: Channels,
    comms: Arc<Comms>,
}

impl PubSubListener {
    /// Create new listener on the server connection.
    pub(crate) fn new(pool: &Pool) -> Self {
        let (tx, mut rx) = mpsc::channel(config().config.general.pub_sub_channel_size);

        let pool = pool.clone();
        let channels = CHANNELS.clone();

        let listener = Self {
            pool: pool.clone(),
            tx,
            channels,
            comms: Arc::new(Comms {
                start: Notify::new(),
                shutdown: Notify::new(),
            }),
        };

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

                    result = Self::run(&pool, &mut rx, channels.clone()) => {
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
    pub(crate) fn launch(&self) {
        self.comms.start.notify_one();
    }

    /// Shutdown the listener.
    pub(crate) fn shutdown(&self) {
        self.comms.shutdown.notify_one();
    }

    /// Listen on a channel.
    pub(crate) async fn listen(
        &self,
        channel: &str,
    ) -> Result<broadcast::Receiver<NotificationResponse>, Error> {
        if let Some(channel) = self.channels.lock().get(channel) {
            return Ok(channel.subscribe());
        }

        let (tx, rx) = broadcast::channel(config().config.general.pub_sub_channel_size);

        self.channels.lock().insert(channel.to_string(), tx);
        self.tx
            .send(Request::Subscribe(channel.to_string()))
            .await
            .map_err(|_| Error::Offline)?;

        Ok(rx)
    }

    /// Notify a channel with payload.
    pub(crate) async fn notify(&self, channel: &str, payload: &str) -> Result<(), Error> {
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
        pool: &Pool,
        rx: &mut mpsc::Receiver<Request>,
        channels: Channels,
    ) -> Result<(), backend::Error> {
        info!("pub/sub started [{}]", pool.addr());

        let mut server = pool.standalone().await?;
        server
            .link_client(&Parameters::from(vec![Parameter {
                name: "application_name".into(),
                value: "PgDog Pub/Sub Listener".into(),
            }]))
            .await?;

        // Re-listen on all channels when re-starting the task.
        // We don't lose LISTEN commands.
        let resub = channels
            .lock()
            .keys()
            .map(|channel| Request::Subscribe(channel.to_string()).into())
            .collect::<Vec<ProtocolMessage>>();
        server.send(&resub.into()).await?;

        loop {
            select! {
                message = server.read() => {
                    let message = message?;

                    // NotificationResponse (B)
                    if message.code() == 'A' {
                        let notification = NotificationResponse::from_bytes(message.to_bytes()?)?;
                        let mut unsub = None;
                        if let Some(channel) = channels.lock().get(notification.channel()) {
                            match channel.send(notification) {
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
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
