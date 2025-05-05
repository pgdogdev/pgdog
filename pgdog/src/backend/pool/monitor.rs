//! Pool monitor and maintenance.
//!
//! # Summary
//!
//! The monitor has three (3) loops running in different Tokio tasks:
//!
//! * the maintenance loop which runs ~3 times per second,
//! * the healthcheck loop which runs every `idle_healthcheck_interval`
//! * the new connection loop which runs every time a client asks
//!   for a new connection to be created
//!
//! ## Maintenance loop
//!
//! The maintenance loop runs every 333ms and removes connections that
//! have been idle for longer than `idle_timeout` and are older than `max_age`.
//!
//! Additionally, the maintenance loop checks the number of clients waiting and
//! triggers the new connection loop to run if there are. This mechanism makes sure
//! that only one connection is created at a time (due to [`tokio::sync::Notify`] storing
//! only a single permit) and prevents the thundering herd problem when many clients request
//! a connection from the pool.
//!
//! ## New connection loop
//!
//! The new connection loop runs every time a client or the maintenance loop request
//! a new connection to be created. This happens when there are no more idle connections
//! in the pool & there are clients waiting for a connection.
//!
//! Only one iteration of this loop can run at a time, so the pool will create one connection
//! at a time and re-evaluate the need for more when it's done creating the connection. Since opening
//! a connection to the server can take ~100ms even inside datacenters, other clients may have returned
//! connections back to the idle pool in that amount of time, and new connections are no longer needed even
//! if clients requested ones to be created ~100ms ago.

use std::time::Duration;

use super::{Error, Guard, Healtcheck, Oids, Pool, Request};
use crate::backend::Server;

use tokio::time::{interval, sleep, timeout, Instant};
use tokio::{select, task::spawn};
use tracing::info;

use tracing::{debug, error};

static MAINTENANCE: Duration = Duration::from_millis(333);

/// Pool maintenance.
///
/// See [`crate::backend::pool::monitor`] module documentation
/// for more details.
pub(super) struct Monitor {
    pool: Pool,
}

impl Monitor {
    /// Launch the pool maintenance loop.
    ///
    /// This is done automatically when the pool is created.
    pub(super) fn run(pool: &Pool) {
        let monitor = Self { pool: pool.clone() };

        spawn(async move {
            monitor.spawn().await;
        });
    }

    /// Run the connection pool.
    async fn spawn(self) {
        debug!("maintenance loop is running [{}]", self.pool.addr());

        // Maintenance loop.
        let pool = self.pool.clone();
        spawn(async move { Self::maintenance(pool).await });
        let pool = self.pool.clone();
        spawn(async move { Self::stats(pool).await });

        // Delay starting healthchecks to give
        // time for the pool to spin up.
        let pool = self.pool.clone();
        let (delay, replication_mode) = {
            let lock = pool.lock();
            let config = lock.config();
            (config.idle_healthcheck_delay(), config.replication_mode)
        };

        if !replication_mode {
            spawn(async move {
                sleep(delay).await;
                Self::healthchecks(pool).await
            });
        }

        loop {
            let comms = self.pool.comms();

            select! {
                // A client is requesting a connection and no idle
                // connections are available.
                _ = comms.request.notified() => {
                    let (
                        should_create,
                        connect_timeout,
                        online,
                    ) = {
                        let mut guard = self.pool.lock();
                        let online = guard.online;

                        if !online {
                            guard.close_waiters(Error::Offline);
                        }

                        (
                            guard.should_create(),
                            guard.config().connect_timeout,
                            guard.online,
                        )
                    };

                    if !online {
                        break;
                    }

                    if should_create {
                        let ok = self.replenish(connect_timeout).await;
                        if !ok {
                            self.pool.ban(Error::ServerError);
                        }
                    }
                }

                // Pool is shutting down.
                _ = comms.shutdown.notified() => {
                    break;
                }
            }
        }

        debug!("maintenance loop is shut down [{}]", self.pool.addr());
    }

    /// The healthcheck loop.
    ///
    /// Runs regularly and ensures the pool triggers healthchecks on idle connections.
    async fn healthchecks(pool: Pool) {
        let mut tick = interval(pool.lock().config().idle_healthcheck_interval());
        let comms = pool.comms();

        debug!("healthchecks running [{}]", pool.addr());

        loop {
            let mut unbanned = false;
            select! {
                _ = tick.tick() => {
                    {
                        let guard = pool.lock();

                        // Pool is offline, exit.
                        if !guard.online {
                            break;
                        }

                        // Pool is paused, skip healtcheck.
                        if guard.paused {
                            continue;
                        }

                    }

                    // If the server is okay, remove the ban if it had one.
                    if let Ok(true) = Self::healthcheck(&pool).await {
                        unbanned = pool.lock().maybe_unban();
                    }
                }


                _ = comms.shutdown.notified() => break,
            }

            if unbanned {
                info!("pool unbanned due to healtcheck [{}]", pool.addr());
            }
        }

        debug!("healthchecks stopped [{}]", pool.addr());
    }

    /// Perform maintenance on the pool periodically.
    async fn maintenance(pool: Pool) {
        let mut tick = interval(MAINTENANCE);
        let comms = pool.comms();

        debug!("maintenance started [{}]", pool.addr());

        loop {
            select! {
                _ = tick.tick() => {
                    let now = Instant::now();

                    let mut guard = pool.lock();

                    if !guard.online {
                        guard.close_waiters(Error::Offline);
                        break;
                    }

                    // If a client is waiting already,
                    // create it a connection.
                    if guard.should_create() {
                        comms.request.notify_one();
                    }

                    // Don't perform any additional maintenance tasks.
                    if guard.paused {
                        continue;
                    }

                    guard.close_idle(now);
                    guard.close_old(now);
                    let unbanned = guard.check_ban(now);

                    if unbanned {
                        info!("pool unbanned due to maintenance [{}]", pool.addr());
                    }
                }

                _ = comms.shutdown.notified() => break,
            }
        }

        debug!("maintenance shut down [{}]", pool.addr());
    }

    /// Replenish pool with one new connection.
    async fn replenish(&self, connect_timeout: Duration) -> bool {
        let mut ok = false;
        let options = self.pool.server_options();

        match timeout(connect_timeout, Server::connect(self.pool.addr(), options)).await {
            Ok(Ok(conn)) => {
                ok = true;
                let server = Box::new(conn);

                let mut guard = self.pool.lock();
                guard.put(server);
            }

            Ok(Err(err)) => {
                error!("error connecting to server: {} [{}]", err, self.pool.addr());
            }

            Err(_) => {
                error!("server connection timeout [{}]", self.pool.addr());
            }
        }

        ok
    }

    #[allow(dead_code)]
    async fn fetch_oids(pool: &Pool) -> Result<(), Error> {
        if pool.lock().oids.is_none() {
            let oids = Oids::load(&mut pool.get(&Request::default()).await?)
                .await
                .ok();
            if let Some(oids) = oids {
                pool.lock().oids = Some(oids);
            }
        }

        Ok(())
    }

    /// Perform a periodic healthcheck on the pool.
    async fn healthcheck(pool: &Pool) -> Result<bool, Error> {
        let (conn, healthcheck_timeout, connect_timeout) = {
            let mut guard = pool.lock();
            if !guard.online || guard.banned() {
                return Ok(false);
            }
            (
                guard.take(&Request::default()),
                guard.config.healthcheck_timeout,
                guard.config.connect_timeout,
            )
        };

        // Have an idle connection, use that for the healthcheck.
        if let Some(conn) = conn {
            Healtcheck::mandatory(
                &mut Guard::new(pool.clone(), conn, Instant::now()),
                pool,
                healthcheck_timeout,
            )
            .healthcheck()
            .await?;

            Ok(true)
        } else {
            // Create a new one and close it. once done.
            info!("creating new healthcheck connection [{}]", pool.addr());
            match timeout(
                connect_timeout,
                Server::connect(pool.addr(), pool.server_options()),
            )
            .await
            {
                Ok(Ok(mut server)) => {
                    Healtcheck::mandatory(&mut server, pool, healthcheck_timeout)
                        .healthcheck()
                        .await?
                }
                Ok(Err(err)) => {
                    error!("healthcheck error: {} [{}]", err, pool.addr());
                }

                Err(_) => {
                    error!("healthcheck timeout [{}]", pool.addr());
                }
            }

            Err(Error::HealthcheckError)
        }
    }

    async fn stats(pool: Pool) {
        let duration = Duration::from_secs(15);
        let comms = pool.comms();

        loop {
            select! {
                _ = sleep(duration) => {
                    {
                        let mut lock = pool.lock();
                        lock.stats.calc_averages(duration);
                    }
                    debug!("calculated averages [{}]", pool.addr());
                }

                _ = comms.shutdown.notified() => {
                    break;
                }
            }
        }
    }
}
