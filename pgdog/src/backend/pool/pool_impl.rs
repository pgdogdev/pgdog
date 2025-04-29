//! Connection pool.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{lock_api::MutexGuard, Mutex, RawMutex};
use rand::{thread_rng, Rng};
use tokio::select;
use tokio::time::sleep;
use tracing::{error, info};

use crate::backend::{Server, ServerOptions};
use crate::net::messages::BackendKeyData;
use crate::net::Parameter;

use super::{
    Address, Comms, Config, Error, Guard, Healtcheck, Inner, Monitor, Oids, PoolConfig, Request,
    State, Waiting,
};

/// Connection pool.
pub struct Pool {
    inner: Arc<Mutex<Inner>>,
    comms: Arc<Comms>,
    addr: Address,
    id: i64,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").field("addr", &self.addr).finish()
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            comms: self.comms.clone(),
            addr: self.addr.clone(),
            id: self.id,
        }
    }
}

impl Pool {
    /// Create new connection pool.
    pub fn new(config: &PoolConfig) -> Self {
        let id = thread_rng().gen();
        Self {
            inner: Arc::new(Mutex::new(Inner::new(config.config, id))),
            comms: Arc::new(Comms::new()),
            addr: config.address.clone(),
            id,
        }
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
    async fn get_internal(&self, request: &Request, mut unban: bool) -> Result<Guard, Error> {
        loop {
            // Fast path, idle connection probably available.
            let (checkout_timeout, healthcheck_timeout, healthcheck_interval, server) = {
                let elapsed = request.created_at.elapsed(); // Before the lock!
                let mut guard = self.lock();

                if !guard.online {
                    return Err(Error::Offline);
                }

                // Try this only once. If the pool still
                // has an error after a checkout attempt,
                // return error.
                if unban {
                    unban = false;
                    guard.maybe_unban();
                }

                if guard.banned() {
                    return Err(Error::Banned);
                }

                let conn = guard
                    .take(request)
                    .map(|server| Guard::new(self.clone(), server));

                if conn.is_some() {
                    guard.stats.counts.wait_time += elapsed.as_micros();
                    guard.stats.counts.server_assignment_count += 1;
                }

                (
                    if guard.paused {
                        Duration::MAX // Wait forever if the pool is paused.
                    } else {
                        guard.config.checkout_timeout()
                    },
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
            let _waiting = Waiting::new(self.clone(), request);

            select! {
                // A connection may be available.
                _ =  self.comms().ready.notified() => {
                    let waited_for = request.created_at.elapsed();
                    if waited_for >= checkout_timeout {
                        return Err(Error::CheckoutTimeout);
                    }
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
        mut conn: Guard,
        healthcheck_timeout: Duration,
        healthcheck_interval: Duration,
    ) -> Result<Guard, Error> {
        let mut healthcheck = Healtcheck::conditional(
            &mut conn,
            self.clone(),
            healthcheck_interval,
            healthcheck_timeout,
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
    pub(super) fn checkin(&self, server: Server) {
        // Ask for the time before locking.
        // This can take some time on some systems, e.g. EC2.
        let now = Instant::now();

        // Check everything and maybe check the connection
        // into the idle pool.
        let banned = self.lock().maybe_check_in(server, now);

        if banned {
            error!(
                "pool banned on check in: {} [{}]",
                Error::ServerError,
                self.addr()
            );
            // Tell everyone to stop waiting, this pool is broken.
            self.comms().ready.notify_waiters();
        }

        // Notify clients that a connection may be available
        // or at least they should request a new one from the pool again.
        self.comms().ready.notify_one();
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
            self.comms().ready.notify_waiters();
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
    pub(crate) fn id(&self) -> i64 {
        self.id
    }

    /// Take connections from the pool and tell all idle ones to be returned
    /// to a new instance of the pool.
    ///
    /// This shuts down the pool.
    pub(crate) fn move_conns_to(&self, destination: &Pool) {
        assert!(self.id != destination.id());

        let idle = {
            let mut guard = self.lock();
            guard.online = false;
            guard.move_conns_to(destination)
        };

        {
            let mut destination_pool = destination.lock();
            for server in idle {
                destination_pool.put(server);
            }
        }

        destination.launch();

        // Notify all waiters on the old pool.
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
        self.comms().shutdown.notify_waiters();
        self.comms().ready.notify_waiters();
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
    #[inline]
    pub fn addr(&self) -> &Address {
        &self.addr
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

        let config = *self.lock().config();

        if let Some(statement_timeout) = config.statement_timeout {
            params.push(Parameter {
                name: "statement_timeout".into(),
                value: statement_timeout.to_string(),
            });
        }

        if config.replication_mode {
            params.push(Parameter {
                name: "replication".into(),
                value: "database".into(),
            });
        }

        ServerOptions { params }
    }

    /// Pool state.
    pub fn state(&self) -> State {
        State::get(self)
    }

    /// Update pool configuration.
    ///
    /// This takes effect immediately.
    pub fn update_config(&self, config: Config) {
        self.lock().config = config;
    }

    /// Fetch OIDs for user-defined data types.
    pub fn oids(&self) -> Option<Oids> {
        self.lock().oids
    }
}
