//! Connection pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::Lazy;
use parking_lot::{lock_api::MutexGuard, Mutex, RawMutex};
use tokio::time::Instant;
use tracing::{error, info};

use crate::backend::{Server, ServerOptions};
use crate::config::PoolerMode;
use crate::net::messages::BackendKeyData;
use crate::net::Parameter;

use super::inner::CheckInResult;
use super::{
    Address, Comms, Config, Error, Guard, Healtcheck, Inner, Monitor, Oids, PoolConfig, Request,
    State, Waiting,
};

static ID_COUNTER: Lazy<Arc<AtomicU64>> = Lazy::new(|| Arc::new(AtomicU64::new(0)));
fn next_pool_id() -> u64 {
    ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Connection pool.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<InnerSync>,
}

pub(crate) struct InnerSync {
    pub(super) comms: Comms,
    pub(super) addr: Address,
    pub(super) inner: Mutex<Inner>,
    pub(super) id: u64,
    pub(super) config: Config,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("addr", &self.inner.addr)
            .finish()
    }
}

impl Pool {
    /// Create new connection pool.
    pub fn new(config: &PoolConfig) -> Self {
        let id = next_pool_id();
        Self {
            inner: Arc::new(InnerSync {
                comms: Comms::new(),
                addr: config.address.clone(),
                inner: Mutex::new(Inner::new(config.config, id)),
                id,
                config: config.config,
            }),
        }
    }

    pub(crate) fn inner(&self) -> &InnerSync {
        &self.inner
    }

    /// Launch the maintenance loop, bringing the pool online.
    pub fn launch(&self) {
        let mut guard = self.lock();
        if !guard.online {
            guard.online = true;
            Monitor::run(self);
        }
    }

    pub async fn get_forced(&self, request: &Request) -> Result<Guard, Error> {
        self.get_internal(request, true).await
    }

    pub async fn get(&self, request: &Request) -> Result<Guard, Error> {
        self.get_internal(request, false).await
    }

    /// Get a connection from the pool.
    async fn get_internal(&self, request: &Request, unban: bool) -> Result<Guard, Error> {
        let pool = self.clone();

        // Fast path, idle connection probably available.
        let (server, granted_at, paused) = {
            // Ask for time before we acquire the lock
            // and only if we actually waited for a connection.
            let granted_at = request.created_at;
            let elapsed = granted_at.saturating_duration_since(request.created_at);
            let mut guard = self.lock();

            if !guard.online {
                return Err(Error::Offline);
            }

            // Try this only once. If the pool still
            // has an error after a checkout attempt,
            // return error.
            if unban && guard.banned() {
                guard.maybe_unban();
            }

            if guard.banned() {
                return Err(Error::Banned);
            }

            let conn = guard.take(request);

            if conn.is_some() {
                guard.stats.counts.wait_time += elapsed;
                guard.stats.counts.server_assignment_count += 1;
            }

            (conn, granted_at, guard.paused)
        };

        if paused {
            self.comms().ready.notified().await;
        }

        let (server, granted_at) = if let Some(mut server) = server {
            (
                Guard::new(
                    pool,
                    {
                        server.set_pooler_mode(self.inner.config.pooler_mode);
                        server
                    },
                    granted_at,
                ),
                granted_at,
            )
        } else {
            // Slow path, pool is empty, will create new connection
            // or wait for one to be returned if the pool is maxed out.
            let waiting = Waiting::new(pool, request)?;
            waiting.wait().await?
        };

        return self
            .maybe_healthcheck(
                server,
                self.inner.config.healthcheck_timeout,
                self.inner.config.healthcheck_interval,
                granted_at,
            )
            .await;
    }

    /// Perform a healtcheck on the connection if one is needed.
    async fn maybe_healthcheck(
        &self,
        mut conn: Guard,
        healthcheck_timeout: Duration,
        healthcheck_interval: Duration,
        now: Instant,
    ) -> Result<Guard, Error> {
        let mut healthcheck = Healtcheck::conditional(
            &mut conn,
            self,
            healthcheck_interval,
            healthcheck_timeout,
            now,
        );

        if let Err(err) = healthcheck.healthcheck().await {
            drop(conn);
            self.ban(Error::HealthcheckError);
            return Err(err);
        }

        Ok(conn)
    }

    /// Create new identical connection pool.
    pub fn duplicate(&self) -> Pool {
        Pool::new(&PoolConfig {
            address: self.addr().clone(),
            config: *self.lock().config(),
        })
    }

    /// Check the connection back into the pool.
    pub(super) fn checkin(&self, mut server: Box<Server>) {
        // Server is checked in right after transaction finished
        // in transaction mode but can be checked in anytime in session mode.
        let now = if server.pooler_mode() == &PoolerMode::Session {
            Instant::now()
        } else {
            server.stats().last_used
        };

        let counts = server.stats_mut().reset_last_checkout();

        // Check everything and maybe check the connection
        // into the idle pool.
        let CheckInResult { banned, replenish } =
            { self.lock().maybe_check_in(server, now, counts) };

        if banned {
            error!(
                "pool banned on check in: {} [{}]",
                Error::ServerError,
                self.addr()
            );
        }

        // Notify maintenance that we need a new connection because
        // the one we tried to check in was broken.
        if replenish {
            self.comms().request.notify_one();
        }
    }

    /// Server connection used by the client.
    pub fn peer(&self, id: &BackendKeyData) -> Option<BackendKeyData> {
        self.lock().peer(id)
    }

    /// Send a cancellation request if the client is connected to a server.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        if let Some(server) = self.peer(id) {
            Server::cancel(self.addr(), &server).await?;
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
            error!("pool banned explicitly: {} [{}]", reason, self.addr());
        }
    }

    /// Unban this pool from serving traffic.
    pub fn unban(&self) {
        let unbanned = self.lock().maybe_unban();
        if unbanned {
            info!("pool unbanned manually [{}]", self.addr());
        }
    }

    /// Connection pool unique identifier.
    pub(crate) fn id(&self) -> u64 {
        self.inner.id
    }

    /// Take connections from the pool and tell all idle ones to be returned
    /// to a new instance of the pool.
    ///
    /// This shuts down the pool.
    pub(crate) fn move_conns_to(&self, destination: &Pool) {
        // Ensure no deadlock.
        assert!(self.inner.id != destination.id());

        {
            let mut from_guard = self.lock();
            let mut to_guard = destination.lock();

            from_guard.online = false;
            let (idle, taken) = from_guard.move_conns_to(destination);
            for server in idle {
                to_guard.put(server);
            }
            to_guard.set_taken(taken);
        }

        destination.launch();
        self.shutdown();
    }

    /// The two pools refer to the same database.
    pub(crate) fn can_move_conns_to(&self, destination: &Pool) -> bool {
        self.addr() == destination.addr()
    }

    /// Pause pool, closing all open connections.
    pub fn pause(&self) {
        let mut guard = self.lock();

        guard.paused = true;
        guard.dump_idle();
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
        guard.dump_idle();
        guard.close_waiters(Error::Offline);
        self.comms().shutdown.notify_waiters();
        self.comms().ready.notify_waiters();
    }

    /// Pool exclusive lock.
    #[inline]
    pub(super) fn lock(&self) -> MutexGuard<'_, RawMutex, Inner> {
        self.inner.inner.lock()
    }

    /// Internal notifications.
    #[inline]
    pub(super) fn comms(&self) -> &Comms {
        &self.inner.comms
    }

    /// Pool address.
    #[inline]
    pub fn addr(&self) -> &Address {
        &self.inner.addr
    }

    /// Get startup parameters for new server connections.
    pub(super) fn server_options(&self) -> ServerOptions {
        let mut params = vec![
            Parameter {
                name: "application_name".into(),
                value: "PgDog".into(),
            },
            Parameter {
                name: "client_encoding".into(),
                value: "utf-8".into(),
            },
        ];

        let config = self.inner.config;

        if let Some(statement_timeout) = config.statement_timeout {
            params.push(Parameter {
                name: "statement_timeout".into(),
                value: statement_timeout.as_millis().to_string(),
            });
        }

        if config.replication_mode {
            params.push(Parameter {
                name: "replication".into(),
                value: "database".into(),
            });
        }

        if config.read_only {
            params.push(Parameter {
                name: "default_transaction_read_only".into(),
                value: "on".into(),
            });
        }

        ServerOptions { params }
    }

    /// Pool state.
    pub fn state(&self) -> State {
        State::get(self)
    }

    /// Update pool configuration used in internals.
    #[cfg(test)]
    pub(crate) fn update_config(&self, config: Config) {
        self.lock().config = config;
    }

    /// Fetch OIDs for user-defined data types.
    pub fn oids(&self) -> Option<Oids> {
        self.lock().oids
    }
}
