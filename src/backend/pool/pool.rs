//! Connection pool.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tokio::{select, spawn};
use tracing::error;

use crate::backend::Server;
use crate::net::messages::BackendKeyData;

use super::{Config, Error, Guard};

static POOL: OnceCell<Pool> = OnceCell::new();

/// Get a connection pool handle.
pub fn pool() -> Pool {
    POOL.get_or_init(Pool::new).clone()
}

#[derive(Debug, Copy, Clone, PartialEq)]
struct Mapping {
    client: BackendKeyData,
    server: BackendKeyData,
}

struct Inner {
    conns: VecDeque<Server>,
    taken: Vec<Mapping>,
    config: Config,
    waiting: usize,
    banned_at: Option<Instant>,
}

struct Comms {
    ready: Notify,
    request: Notify,
    shutdown: Notify,
}

struct Waiting {
    pool: Pool,
}

impl Waiting {
    fn new(pool: Pool) -> Self {
        pool.inner.lock().waiting += 1;
        Self { pool }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.pool.inner.lock().waiting -= 1;
    }
}

/// Connection pool.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<Mutex<Inner>>,
    comms: Arc<Comms>,
}

impl Pool {
    /// Create new connection pool.
    pub fn new() -> Self {
        let pool = Self {
            inner: Arc::new(Mutex::new(Inner {
                conns: VecDeque::new(),
                taken: Vec::new(),
                config: Config::default(),
                waiting: 0,
                banned_at: None,
            })),
            comms: Arc::new(Comms {
                ready: Notify::new(),
                request: Notify::new(),
                shutdown: Notify::new(),
            }),
        };

        let custodian = pool.clone();
        spawn(async move {
            custodian.spawn().await;
        });

        pool
    }

    /// Get a connetion from the pool.
    pub async fn get(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        loop {
            let config = {
                let mut guard = self.inner.lock();
                if let Some(server) = guard.conns.pop_back() {
                    guard.taken.push(Mapping {
                        client: id.clone(),
                        server: server.id().clone(),
                    });

                    return Ok(Guard::new(self.clone(), server));
                }

                guard.config.clone()
            };

            self.comms.request.notify_one();
            let _waiting = Waiting::new(self.clone());

            select! {
                _ =  self.comms.ready.notified() => {
                    continue;
                }

                _ = sleep(config.checkout_timeout()) => {
                    return Err(Error::CheckoutTimeout);
                }
            }
        }
    }

    /// Run the connection pool.
    async fn spawn(self) {
        loop {
            select! {
                _ = self.comms.request.notified() => {
                    let (available, total, config) = {
                        let guard = self.inner.lock();
                        let total = guard.conns.len() + guard.taken.len();
                        (!guard.conns.is_empty(), total, guard.config.clone())
                    };

                    let can_create_more = total < config.max;

                    if available {
                        self.comms.ready.notify_one();
                    } else if can_create_more {
                        match timeout(config.connect_timeout(), Server::connect("127.0.0.1:5432")).await {
                            Ok(Ok(conn)) => {
                                let mut guard = self.inner.lock();
                                guard.conns.push_front(conn);

                                self.comms.ready.notify_one();
                            }

                            Ok(Err(err)) => {
                                error!("error connecting to server: {:?}", err);
                            }

                            Err(_) => {
                                error!("server connection timeout");
                            }
                        }
                    }
                }

                _ = self.comms.shutdown.notified() => {
                    break;
                }

                // Perform maintenance ~3 times per second.
                _ = sleep(Duration::from_millis(333)) => {
                    let now = Instant::now();
                    let mut guard = self.inner.lock();
                    let config = guard.config.clone();

                    // Remove idle connections.
                    let mut remove = std::cmp::max(0, guard.conns.len() as i64 - config.min as i64);
                    guard.conns.retain(|c| {
                        let age = c.age(now);
                        if remove <= 0 {
                            true
                        } else {
                            if age >= config.idle_timeout() {
                                remove -= 1;
                                false
                            } else {
                                true
                            }
                        }
                    });

                    // Remove connections based on max age.
                    guard.conns.retain(|c| {
                        let age = c.age(now);
                        age < config.max_age()
                    });

                    // If we have clients waiting still, try to open a connection again.
                    if guard.waiting > 0 {
                        self.comms.request.notify_one();
                    }

                    // Create a new connection to bring up the minimum open connections amount.
                    if guard.conns.len() + guard.taken.len() < guard.config.min {
                        self.comms.request.notify_one();
                    }
                }
            }
        }
    }

    /// Check the connection back into the pool.
    pub(super) fn checkin(&self, server: Server) {
        let now = Instant::now();
        let mut guard = self.inner.lock();
        let id = server.id().clone();
        let too_old = server.age(now).as_millis() >= guard.config.max_age as u128;

        if server.done() && !too_old {
            guard.conns.push_back(server);
        } else if server.error() {
            guard.banned_at = Some(Instant::now());
        }

        let index = guard
            .taken
            .iter()
            .enumerate()
            .find(|(_i, p)| p.server == id)
            .map(|(i, _p)| i);

        if let Some(index) = index {
            guard.taken.remove(index);
        }

        self.comms.ready.notify_one();
    }

    /// Server connection used by the client.
    pub fn peer(&self, id: &BackendKeyData) -> Option<BackendKeyData> {
        self.inner
            .lock()
            .taken
            .iter()
            .find(|p| p.client == *id)
            .map(|p| p.server)
    }

    /// Send a cancellation request if the client is connected to a server.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(server) = self.peer(id) {
            Server::cancel("127.0.0.1:5432", &server).await?;
        }

        Ok(())
    }
}
