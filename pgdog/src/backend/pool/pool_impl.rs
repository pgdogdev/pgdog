//! Connection pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use once_cell::sync::{Lazy, OnceCell};
use parking_lot::RwLock;
use parking_lot::{lock_api::MutexGuard, Mutex, RawMutex};
use tokio::time::{timeout, Instant};
use tracing::error;

use crate::backend::pool::LsnStats;
use crate::backend::{ConnectReason, DisconnectReason, Server, ServerOptions};
use crate::config::PoolerMode;
use crate::net::messages::BackendKeyData;
use crate::net::{Parameter, Parameters};

use super::inner::CheckInResult;
use super::{
    lb::TargetHealth, lsn_monitor::LsnMonitor, Address, Comms, Config, Error, Guard, Healtcheck,
    Inner, Monitor, Oids, PoolConfig, Request, State, Waiting,
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
    pub(super) health: TargetHealth,
    pub(super) params: OnceCell<Parameters>,
    pub(super) lsn_stats: RwLock<LsnStats>,
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
                health: TargetHealth::new(id),
                params: OnceCell::new(),
                lsn_stats: RwLock::new(LsnStats::default()),
            }),
        }
    }

    /// Test pool, no connections.
    #[cfg(test)]
    pub fn new_test() -> Self {
        let config = PoolConfig {
            address: Address::new_test(),
            config: Config::default(),
        };

        Self::new(&config)
    }

    pub(crate) fn inner(&self) -> &InnerSync {
        &self.inner
    }

    pub fn healthy(&self) -> bool {
        self.inner.health.healthy()
    }

    /// Launch the maintenance loop, bringing the pool online.
    pub fn launch(&self) {
        let mut guard = self.lock();
        if !guard.online {
            guard.online = true;
            Monitor::run(self);
            LsnMonitor::run(self);
        }
    }

    pub async fn get(&self, request: &Request) -> Result<Guard, Error> {
        match timeout(self.config().checkout_timeout, self.get_internal(request)).await {
            Ok(Ok(conn)) => Ok(conn),
            Err(_) => {
                self.inner.health.toggle(false);
                Err(Error::CheckoutTimeout)
            }
            Ok(Err(err)) => {
                self.inner.health.toggle(false);
                Err(err)
            }
        }
    }

    /// Get a connection from the pool.
    async fn get_internal(&self, request: &Request) -> Result<Guard, Error> {
        loop {
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

                let conn = guard.take(request)?;

                if conn.is_some() {
                    guard.stats.counts.wait_time += elapsed;
                    guard.stats.counts.server_assignment_count += 1;
                    if request.read {
                        guard.stats.counts.reads += 1;
                    } else {
                        guard.stats.counts.writes += 1;
                    }
                }

                (conn, granted_at, guard.paused)
            };

            if paused {
                self.comms().ready.notified().await;
            }

            let (server, granted_at) = if let Some(mut server) = server {
                server
                    .prepared_statements_mut()
                    .set_capacity(self.inner.config.prepared_statements_limit);
                server.set_pooler_mode(self.inner.config.pooler_mode);
                (Guard::new(pool, server, granted_at), granted_at)
            } else {
                // Slow path, pool is empty, will create new connection
                // or wait for one to be returned if the pool is maxed out.
                let mut waiting = Waiting::new(pool, request)?;
                waiting.wait().await?
            };

            match self
                .maybe_healthcheck(
                    server,
                    self.inner.config.healthcheck_timeout,
                    self.inner.config.healthcheck_interval,
                    granted_at,
                )
                .await
            {
                Ok(conn) => return Ok(conn),
                // Try another connection.
                Err(Error::HealthcheckError) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Get server parameters, fetch them if necessary.
    pub async fn params(&self, request: &Request) -> Result<&Parameters, Error> {
        if let Some(params) = self.inner.params.get() {
            Ok(params)
        } else {
            let conn = self.get(request).await?;
            let params = conn.params().clone();
            Ok(self.inner.params.get_or_init(|| params))
        }
    }

    /// Perform a health check on the connection if one is needed.
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
            conn.disconnect_reason(DisconnectReason::Unhealthy);
            drop(conn);
            self.inner.health.toggle(false);
            return Err(err);
        } else if !self.inner.health.healthy() {
            self.inner.health.toggle(true);
        }

        Ok(conn)
    }

    /// Check the connection back into the pool.
    pub(super) fn checkin(&self, mut server: Box<Server>) -> Result<(), Error> {
        // Server is checked in right after transaction finished
        // in transaction mode but can be checked in anytime in session mode.
        let now = if server.pooler_mode() == &PoolerMode::Session {
            Instant::now()
        } else {
            server.stats().last_used()
        };

        let counts = {
            let stats = server.stats_mut();
            stats.clear_client_id();
            let counts = stats.reset_last_checkout();
            stats.update();
            counts
        };

        // Check everything and maybe check the connection
        // into the idle pool.
        let CheckInResult {
            server_error,
            replenish,
        } = { self.lock().maybe_check_in(server, now, counts)? };

        if server_error {
            error!(
                "pool received broken server connection, closing [{}]",
                self.addr()
            );
            self.inner.health.toggle(false);
        }

        // Notify maintenance that we need a new connection because
        // the one we tried to check in was broken.
        if replenish {
            self.comms().request.notify_one();
        }

        Ok(())
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

    /// Pool is available to serve connections.
    pub fn available(&self) -> bool {
        let guard = self.lock();
        !guard.paused && guard.online
    }

    /// Connection pool unique identifier.
    #[inline]
    pub(crate) fn id(&self) -> u64 {
        self.inner.id
    }

    /// Take connections from the pool and tell all idle ones to be returned
    /// to a new instance of the pool.
    ///
    /// This shuts down the pool.
    pub(crate) fn move_conns_to(&self, destination: &Pool) -> Result<(), Error> {
        // Ensure no deadlock.
        assert!(self.inner.id != destination.id());
        let now = Instant::now();

        {
            let mut from_guard = self.lock();
            let mut to_guard = destination.lock();

            from_guard.online = false;
            let (idle, taken) = from_guard.move_conns_to(destination);
            for server in idle {
                to_guard.put(server, now)?;
            }
            to_guard.set_taken(taken);
        }

        destination.launch();
        self.shutdown();

        Ok(())
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

    /// Send a cancellation request for all running queries.
    pub async fn cancel_all(&self) -> Result<(), Error> {
        let taken = self.lock().checked_out_server_ids();
        let addr = self.addr().clone();

        try_join_all(taken.iter().map(|id| Server::cancel(&addr, id)))
            .await
            .map_err(|_| Error::FastShutdown)?;

        Ok(())
    }

    /// Resume the pool.
    pub fn resume(&self) {
        {
            let mut guard = self.lock();
            guard.paused = false;
        }

        self.comms().ready.notify_waiters();
    }

    /// Create a connection to the pool, untracked by the logic here.
    pub async fn standalone(&self, reason: ConnectReason) -> Result<Server, Error> {
        Monitor::create_connection(self, reason).await
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

    /// Get pool configuration.
    #[inline]
    pub fn config(&self) -> &Config {
        &self.inner.config
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
                value: statement_timeout.as_millis().to_string().into(),
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

        ServerOptions {
            params,
            pool_id: self.id(),
        }
    }

    /// Pool state.
    pub fn state(&self) -> State {
        State::get(self)
    }

    /// LSN stats
    pub fn lsn_stats(&self) -> LsnStats {
        *self.inner().lsn_stats.read()
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
