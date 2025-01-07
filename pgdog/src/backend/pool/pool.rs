//! Connection pool.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{lock_api::MutexGuard, Mutex, RawMutex};
use tokio::select;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{error, info};

use crate::backend::Server;
use crate::net::messages::BackendKeyData;

use super::{Address, Ban, Config, Error, Guard, Healtcheck, Inner, Monitor, PoolConfig};

/// Mapping between a client and a server.
#[derive(Debug, Copy, Clone, PartialEq)]
pub(super) struct Mapping {
    /// Client ID.
    pub(super) client: BackendKeyData,
    /// Server ID.
    pub(super) server: BackendKeyData,
}

/// Internal pool notifications.
pub(super) struct Comms {
    /// An idle connection is available in the pool.
    pub(super) ready: Notify,
    /// A client requests a new connection to be open
    /// or waiting for one to be returned to the pool.
    pub(super) request: Notify,
    /// Pool is shutting down.
    pub(super) shutdown: Notify,
}

impl Comms {
    /// Create new comms.
    pub(super) fn new() -> Self {
        Self {
            ready: Notify::new(),
            request: Notify::new(),
            shutdown: Notify::new(),
        }
    }
}

/// Pool state.
pub struct State {
    /// Number of connections checked out.
    pub checked_out: usize,
    /// Number of idle connections.
    pub idle: usize,
    /// Total number of connections managed by the pool.
    pub total: usize,
    /// Is the pool online?
    pub online: bool,
    /// Pool has no idle connections.
    pub empty: bool,
    /// Pool configuration.
    pub config: Config,
    /// The pool is paused.
    pub paused: bool,
    /// Number of clients waiting for a connection.
    pub waiting: usize,
    /// Pool ban.
    pub ban: Option<Ban>,
    /// Pool is banned.
    pub banned: bool,
}

struct Waiting {
    pool: Pool,
}

impl Waiting {
    fn new(pool: Pool) -> Self {
        pool.lock().waiting += 1;
        Self { pool }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.pool.lock().waiting -= 1;
    }
}

/// Connection pool.
pub struct Pool {
    inner: Arc<Mutex<Inner>>,
    comms: Arc<Comms>,
    addr: Address,
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            comms: self.comms.clone(),
            addr: self.addr.clone(),
        }
    }
}

impl Pool {
    /// Create new connection pool.
    pub fn new(config: PoolConfig) -> Self {
        let pool = Self {
            inner: Arc::new(Mutex::new(Inner {
                conns: VecDeque::new(),
                taken: Vec::new(),
                config: config.config,
                waiting: 0,
                ban: None,
                online: true,
                paused: false,
                creating: 0,
            })),
            comms: Arc::new(Comms::new()),
            addr: config.address,
        };

        // Launch the maintenance loop.
        Monitor::new(&pool);

        pool
    }

    /// Get a connetion from the pool.
    pub async fn get(&self, id: &BackendKeyData) -> Result<Guard, Error> {
        loop {
            // Fast path, idle connection probably available.
            let (checkout_timeout, healthcheck_timeout, healthcheck_interval, server) = {
                let mut guard = self.lock();

                if !guard.online {
                    return Err(Error::Offline);
                }

                let conn = if let Some(server) = guard.conns.pop_back() {
                    guard.taken.push(Mapping {
                        client: *id,
                        server: *server.id(),
                    });

                    Some(Guard::new(self.clone(), server))
                } else {
                    None
                };

                (
                    guard.config.checkout_timeout(),
                    guard.config.healthcheck_timeout(),
                    guard.config.healthcheck_interval(),
                    conn,
                )
            };

            if let Some(server) = server {
                return self
                    .maybe_healthcheck(server, healthcheck_timeout, healthcheck_interval)
                    .await;
            }

            // Slow path, pool is empty, will create new connection
            // or wait for one to be returned if the pool is maxed out.
            self.comms().request.notify_one();
            let _waiting = Waiting::new(self.clone());

            select! {
                // A connection may be available.
                _ =  self.comms().ready.notified() => {
                    continue;
                }

                // Waited too long, return an error.
                _ = sleep(checkout_timeout) => {
                    self.lock()
                        .maybe_ban(Instant::now(), Error::CheckoutTimeout);
                    return Err(Error::CheckoutTimeout);
                }
            }
        }
    }

    /// Perform a healtcheck on the connection if one is needed.
    async fn maybe_healthcheck(
        &self,
        conn: Guard,
        healthcheck_timeout: Duration,
        healthcheck_interval: Duration,
    ) -> Result<Guard, Error> {
        let healthcheck = Healtcheck::conditional(
            conn,
            self.clone(),
            healthcheck_interval,
            healthcheck_timeout,
        );

        healthcheck.healtcheck().await
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Pool {
        Pool::new(PoolConfig {
            address: self.addr().clone(),
            config: *self.lock().config(),
        })
    }

    /// Check the connection back into the pool.
    pub(super) fn checkin(&self, server: Server) {
        // Ask for the time before locking.
        // This can take some time on some systems, e.g. EC2.
        let now = Instant::now();

        // Check everything and maybe check the connection
        // into the idle pool.
        let banned = self.lock().maybe_check_in(server, now);

        if banned {
            error!("pool banned: {} [{}]", Error::ServerError, self.addr());
        }

        // Notify clients that a connection may be available
        // or at least they should request a new one from the pool again.
        self.comms().ready.notify_one();
    }

    /// Server connection used by the client.
    pub fn peer(&self, id: &BackendKeyData) -> Option<BackendKeyData> {
        self.lock()
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
        self.lock().banned()
    }

    /// Pool is available to serve connections.
    pub fn available(&self) -> bool {
        let guard = self.lock();
        !guard.paused && guard.online
    }

    /// Ban this connection pool from serving traffic.
    pub fn ban(&self, reason: Error) {
        let now = Instant::now();
        let banned = self.lock().maybe_ban(now, reason);

        if banned {
            error!("pool banned: {} [{}]", reason, self.addr());
        }
    }

    /// Unban this pool from serving traffic.
    pub fn unban(&self) {
        let unbanned = self.lock().maybe_unban();
        if unbanned {
            info!("pool unbanned [{}]", self.addr());
        }
    }

    /// Pause pool, closing all open connections.
    pub fn pause(&self) {
        let mut guard = self.lock();

        guard.paused = true;
        guard.conns.clear();
    }

    /// Resume the pool.
    pub fn resume(&self) {
        {
            let mut guard = self.lock();
            guard.paused = false;
            guard.ban = None;
        }

        self.comms().ready.notify_waiters();
    }

    /// Shutdown the pool.
    pub fn shutdown(&self) {
        let mut guard = self.lock();
        guard.online = false;
        guard.conns.clear();
        self.comms().shutdown.notify_waiters();
    }

    /// Pool exclusive lock.
    #[inline]
    pub(super) fn lock(&self) -> MutexGuard<'_, RawMutex, Inner> {
        self.inner.lock()
    }

    /// Internal notifications.
    #[inline]
    pub(super) fn comms(&self) -> &Comms {
        &self.comms
    }

    /// Pool address.
    pub(crate) fn addr(&self) -> &Address {
        &self.addr
    }

    /// Pool state.
    pub fn state(&self) -> State {
        let guard = self.lock();

        State {
            checked_out: guard.checked_out(),
            idle: guard.idle(),
            total: guard.total(),
            online: guard.online,
            empty: guard.idle() == 0,
            config: guard.config,
            paused: guard.paused,
            waiting: guard.waiting,
            ban: guard.ban,
            banned: guard.ban.is_some(),
        }
    }
}
