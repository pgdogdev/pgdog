//! Connection pool.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    POOL.get_or_init(|| Pool::new("127.0.0.1:5432")).clone()
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
    ban: Option<Ban>,
    online: bool,
    paused: bool,
}

struct Comms {
    ready: Notify,
    request: Notify,
    shutdown: Notify,
    resume: Notify,
    ref_count: AtomicUsize,
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

#[derive(Debug)]
struct Ban {
    created_at: Instant,
    reason: Error,
}

impl Ban {
    fn expired(&self, now: Instant) -> bool {
        now.duration_since(self.created_at) > Duration::from_secs(300)
    }
}

/// Connection pool.
pub struct Pool {
    inner: Arc<Mutex<Inner>>,
    comms: Arc<Comms>,
    addr: String,
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        let clone = Self {
            inner: self.inner.clone(),
            comms: self.comms.clone(),
            addr: self.addr.clone(),
        };

        self.comms.ref_count.fetch_add(1, Ordering::Relaxed);

        clone
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        let remaining = self.comms.ref_count.fetch_sub(1, Ordering::Relaxed);
        if remaining == 1 {
            self.comms.shutdown.notify_one();
        }
    }
}

impl Pool {
    /// Create new connection pool.
    pub fn new(addr: &str) -> Self {
        let pool = Self {
            inner: Arc::new(Mutex::new(Inner {
                conns: VecDeque::new(),
                taken: Vec::new(),
                config: Config::default(),
                waiting: 0,
                ban: None,
                online: true,
                paused: false,
            })),
            comms: Arc::new(Comms {
                ready: Notify::new(),
                request: Notify::new(),
                shutdown: Notify::new(),
                resume: Notify::new(),
                ref_count: AtomicUsize::new(0),
            }),
            addr: addr.to_owned(),
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
                        client: *id,
                        server: *server.id(),
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

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Pool {
        Pool::new(&self.addr)
    }

    /// Run the connection pool.
    async fn spawn(self) {
        loop {
            select! {
                _ = self.comms.request.notified() => {
                    let (available, total, config, paused) = {
                        let guard = self.inner.lock();
                        let total = guard.conns.len() + guard.taken.len();
                        (!guard.conns.is_empty(), total, guard.config.clone(), guard.paused)
                    };

                    if paused {
                        continue;
                    }

                    let can_create_more = total < config.max;

                    if available {
                        self.comms.ready.notify_one();
                    } else if can_create_more {
                        match timeout(config.connect_timeout(), Server::connect(&self.addr)).await {
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
                    self.inner.lock().online = false;
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
                        let idle_for = c.idle_for(now);
                        if remove <= 0 {
                            true
                        } else if idle_for >= config.idle_timeout() {
                            remove -= 1;
                            false
                        } else {
                            true
                        }
                    });

                    // Remove connections based on max age.
                    guard.conns.retain(|c| {
                        let age = c.age(now);
                        age < config.max_age()
                    });

                    // Unban if ban expired.
                    if let Some(ban) = guard.ban.take() {
                        if !ban.expired(now) {
                            guard.ban = Some(ban);
                        }
                    }

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
        let id = *server.id();
        let too_old = server.age(now).as_millis() >= guard.config.max_age as u128;

        if server.done() && !too_old && guard.online && !guard.paused {
            guard.conns.push_back(server);
        } else if server.error() {
            guard.ban = Some(Ban {
                created_at: Instant::now(),
                reason: Error::ServerError,
            });
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

    /// Is this pool banned?
    pub fn banned(&self) -> bool {
        self.inner.lock().ban.is_some()
    }

    /// Pool is available to serve connections.
    pub fn available(&self) -> bool {
        let guard = self.inner.lock();
        !guard.paused && guard.online && guard.ban.is_none()
    }

    /// Ban this connection pool from serving traffic.
    pub fn ban(&self) {
        self.inner.lock().ban = Some(Ban {
            created_at: Instant::now(),
            reason: Error::ManualBan,
        });
    }

    /// Unban this pool from serving traffic.
    pub fn unban(&self) {
        self.inner.lock().ban = None;
    }

    /// Pause pool.
    pub fn pause(&self) {
        self.inner.lock().paused = true;
    }

    /// Wait for pool to resume if it's paused.
    pub async fn wait_resume(&self) {
        if self.inner.lock().paused {
            self.comms.resume.notified().await;
        }
    }

    /// Resume the pool.
    pub fn resume(&self) {
        self.inner.lock().paused = false;
        self.comms.resume.notify_waiters();
    }
}
