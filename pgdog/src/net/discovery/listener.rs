//! Service discovery listener.

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rand::Rng;
use tracing::{debug, error, info};

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::net::UdpSocket;
use tokio::time::{interval, Duration};
use tokio::{select, spawn};

use super::{Error, Message, Payload};

/// Service discovery listener.
#[derive(Clone, Debug)]
pub struct Listener {
    id: u64,
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Clone)]
pub struct State {
    /// Number of connected clients.
    pub clients: u64,
    /// When we received the last state update.
    pub last_message: SystemTime,
}

#[derive(Debug)]
struct Inner {
    peers: HashMap<SocketAddr, State>,
}

static LISTENER: Lazy<Listener> = Lazy::new(Listener::new);

impl Listener {
    /// Create new listener.
    fn new() -> Self {
        Self {
            id: rand::thread_rng().gen(),
            inner: Arc::new(Mutex::new(Inner {
                peers: HashMap::new(),
            })),
        }
    }

    /// Get listener.
    pub fn get() -> Self {
        LISTENER.clone()
    }

    /// Get peers.
    pub fn peers(&self) -> HashMap<SocketAddr, State> {
        self.inner.lock().peers.clone()
    }

    /// Run the listener.
    pub fn run(&self, address: Ipv4Addr, port: u16) {
        let listener = self.clone();
        info!("launching service discovery ({}:{})", address, port);
        spawn(async move {
            if let Err(err) = listener.spawn(address, port).await {
                error!("crashed: {:?}", err);
            }
        });
    }

    /// Run listener.
    pub async fn spawn(&self, address: Ipv4Addr, port: u16) -> Result<Self, Error> {
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", port)).await?;
        socket.join_multicast_v4(address, "0.0.0.0".parse::<Ipv4Addr>().unwrap())?;
        socket.multicast_loop_v4()?; // Won't work on IPv6, but nice for debugging.

        let mut buf = vec![0u8; 1024];
        let mut interval = interval(Duration::from_secs(1));

        loop {
            select! {
                result = socket.recv_from(&mut buf) => {
                    let (len, addr) = result?;
                    let message = Message::from_bytes(&buf[..len]).ok();
                    let now = SystemTime::now();

                    if let Some(message) = message {
                        debug!("{}: {:#?}", addr, message);

                        if let Payload::Stats {
                                clients
                            } = message.payload {
                            self.inner.lock().peers.insert(addr, State {
                                clients,
                                last_message: now,
                            });
                        }

                    }
                }

                _ = interval.tick() => {
                    let healthcheck = Message::stats(self.id).to_bytes()?;
                    socket.send_to(&healthcheck, format!("{}:{}", address, port)).await?;
                    debug!("healtcheck");
                }
            }
        }
    }
}
