//! Pool monitor and maintenance.
//!
//! # Summary
//!
//! The monitor has four (4) loops running in different Tokio tasks:
//!
//! * the maintenance loop which runs ~3 times per second,
//! * the healthcheck loop which runs every `idle_healthcheck_interval`,
//! * the new connection loop which runs every time a client asks
//!   for a new connection to be created,
//! * the token refresh loop which runs for pools backed by an external
//!   identity provider (RDS IAM, Azure Workload Identity).
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
//!
//! ## Token refresh loop
//!
//! Spawned once per pool for addresses that use an external identity provider.
//! Sleeps until just before the cached token expires, fetches a new one, and
//! repeats. On failure it evicts the stale cache entry so the next connection
//! attempt blocks on a fresh fetch rather than presenting an expired token.
//! The loop exits when the pool shuts down (e.g. on config reload), preventing
//! refresh tasks from leaking across reloads.

use std::time::Duration;

use super::{Error, Guard, Healtcheck, Oids, Pool, Request};
use crate::backend::auth::{azure_workload_identity, rds_iam};
use crate::backend::pool::inner::ShouldCreate;
use crate::backend::pool::token_cache::TokenCache;
use crate::backend::{ConnectReason, DisconnectReason, Server};
use crate::config::ServerAuth;

use tokio::time::{Instant, interval, sleep, timeout};
use tokio::{select, task::spawn};
use tracing::{debug, error, info, warn};

static MAINTENANCE: Duration = Duration::from_millis(333);

/// Pool maintenance.
///
/// See [`crate::backend::pool::monitor`] module documentation
/// for more details.
pub struct Monitor {
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

        // Delay starting health checks to give
        // time for the pool to spin up.
        let pool = self.pool.clone();
        let (delay, interval, replication_mode) = {
            let lock = pool.lock();
            let config = lock.config();
            (
                config.idle_healthcheck_delay(),
                config.idle_healthcheck_interval(),
                config.replication_mode,
            )
        };

        if !replication_mode && interval > Duration::ZERO {
            spawn(async move {
                sleep(delay).await;
                Self::healthchecks(pool).await
            });
        }

        // Token refresh loop — one task per pool, tied to pool lifetime.
        // Only spawned for pools that use an external identity provider.
        if self.pool.addr().server_auth.is_external_identity() {
            let pool = self.pool.clone();
            spawn(async move { Self::token_refresh(pool).await });
        }

        loop {
            let comms = self.pool.comms();

            select! {
                // A client is requesting a connection and no idle
                // connections are available.
                _ = comms.request.notified() => {
                    let (
                        should_create,
                        online,
                    ) = {
                        let mut guard = self.pool.lock();
                        let online = guard.online;

                        if !online {
                            guard.close_waiters(Error::Offline);
                        }

                        (
                            guard.should_create(),
                            guard.online,
                        )
                    };

                    if !online {
                        break;
                    }

                    if let ShouldCreate::Yes { reason, .. } = should_create {
                        info!("new connection requested: {} [{}]", should_create, self.pool.addr());
                        let ok = match self.replenish(reason).await {
                            Ok(r) => r,
                            Err(err) => {
                                error!("monitor error: {}", err);
                                if err.is_auth() {
                                    warn!("Authentication failed with backend database, shutting down pool [{}] to prevent connection storm", self.pool.addr());
                                    self.pool.shutdown();
                                }
                                false
                            }
                        };
                        if !ok {
                            self.pool.inner().health.toggle(false);
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

    /// Keeps the token cache warm for this pool's address.
    ///
    /// Sleeps until just before the cached token expires, fetches a fresh
    /// one, and repeats. On failure the stale entry is evicted so the next
    /// [`TokenCache::get_or_fetch`] call blocks on a real fetch rather than
    /// handing out an expired token indefinitely.
    ///
    /// Exits when the pool shuts down, so no refresh tasks outlive their
    /// pool across config reloads.
    async fn token_refresh(pool: Pool) {
        let addr = pool.addr().clone();
        let comms = pool.comms();

        debug!("token refresh loop started [{}]", addr);

        loop {
            // Sleep until just before the cached token expires.
            // If nothing is cached yet (cold start) or the entry was evicted
            // after a failed refresh, fire immediately to prime the cache.
            let sleep_duration = TokenCache::global().refresh_in(&addr);

            select! {
                _ = sleep(sleep_duration) => {
                    let result = match addr.server_auth {
                        ServerAuth::RdsIam => rds_iam::token(addr.clone()).await,
                        ServerAuth::AzureWorkloadIdentity => {
                            azure_workload_identity::token(addr.clone()).await
                        }
                        // Guard in spawn() ensures we only reach here for
                        // external identity pools.
                        _ => break,
                    };

                    match result {
                        Ok((token, expires_at)) => {
                            debug!("token refreshed [{}]", addr);
                            TokenCache::global().set(&addr, token, expires_at);
                        }
                        Err(err) => {
                            warn!("token refresh failed, evicting cache entry: {err} [{}]", addr);
                            TokenCache::global().evict(&addr);
                        }
                    }
                }

                _ = comms.shutdown.notified() => break,
            }
        }

        debug!("token refresh loop stopped [{}]", addr);
    }

    /// The health check loop.
    ///
    /// Runs regularly and ensures the pool triggers health checks on idle connections.
    async fn healthchecks(pool: Pool) {
        let mut tick = interval(pool.lock().config().idle_healthcheck_interval());
        let comms = pool.comms();

        debug!("health checks running [{}]", pool.addr());

        loop {
            select! {
                _ = tick.tick() => {
                    {
                        let guard = pool.lock();

                        // Pool is offline, exit.
                        if !guard.online {
                            break;
                        }

                        // Pool is paused, skip health check.
                        if guard.paused {
                            continue;
                        }
                    }

                    let _ = Self::healthcheck(&pool).await;
                }


                _ = comms.shutdown.notified() => break,
            }
        }

        debug!("health checks stopped [{}]", pool.addr());
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
                    if guard.should_create().yes() {
                        comms.request.notify_one();
                    }

                    // Don't perform any additional maintenance tasks.
                    if guard.paused {
                        continue;
                    }

                    guard.close_idle(now);
                    guard.close_old(now);
                }

                _ = comms.shutdown.notified() => break,
            }
        }

        debug!("maintenance shut down [{}]", pool.addr());
    }

    /// Replenish pool with one new connection.
    async fn replenish(&self, reason: ConnectReason) -> Result<bool, Error> {
        match Self::create_connection(&self.pool, reason).await {
            Ok(conn) => {
                let now = Instant::now();
                let server = Box::new(conn);
                let mut guard = self.pool.lock();
                if guard.online {
                    guard.put(server, now)?;
                }
                Ok(true)
            }
            Err(err) => Err(err),
        }
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

    pub async fn healthcheck(pool: &Pool) -> Result<bool, Error> {
        match Self::healthcheck_internal(pool).await {
            Ok(result) => {
                pool.inner().health.toggle(result);
                Ok(result)
            }

            Err(err) => {
                pool.inner().health.toggle(false);
                Err(err)
            }
        }
    }

    /// Perform a periodic healthcheck on the pool.
    async fn healthcheck_internal(pool: &Pool) -> Result<bool, Error> {
        let conn = {
            let mut guard = pool.lock();
            if !guard.online {
                return Ok(false);
            }
            guard.take(&Request::default())?
        };

        let healthcheck_timeout = pool.config().healthcheck_timeout;

        // Have an idle connection, use that for the healthcheck.
        if let Some(conn) = conn {
            Healtcheck::mandatory(
                &mut Guard::new(pool.clone(), conn, Instant::now()),
                pool,
                healthcheck_timeout,
            )
            .healthcheck()
            .await?;
        } else {
            // Create a new one and close it.
            info!("creating new healthcheck connection [{}]", pool.addr());

            let mut server = Self::create_connection(pool, ConnectReason::Healthcheck)
                .await
                .map_err(|_| Error::HealthcheckError)?;

            server.disconnect_reason(DisconnectReason::Healthcheck);

            Healtcheck::mandatory(&mut server, pool, healthcheck_timeout)
                .healthcheck()
                .await?;
        }

        Ok(true)
    }

    async fn stats(pool: Pool) {
        let duration = pool.config().stats_period;
        let comms = pool.comms();

        // Don't tick that often.
        if duration.is_zero() {
            return;
        }

        let mut interval = interval(pool.config().stats_period);

        loop {
            select! {
                _ = interval.tick() => {
                    {
                        let mut lock = pool.lock();
                        lock.stats.calc_averages(duration);
                    }
                }

                _ = comms.shutdown.notified() => {
                    break;
                }
            }
        }
    }

    pub(super) async fn create_connection(
        pool: &Pool,
        reason: ConnectReason,
    ) -> Result<Server, Error> {
        let connect_timeout = pool.config().connect_timeout;
        let connect_attempts = pool.config().connect_attempts;
        let connect_attempt_delay = pool.config().connect_attempt_delay;
        let options = pool.server_options();

        let mut error = Error::ServerError;
        let now = Instant::now();

        let max_age = pool.config().max_age;
        let max_age_jitter = pool.config().max_age_jitter;

        for attempt in 0..connect_attempts {
            match timeout(
                connect_timeout,
                Server::connect(pool.addr(), options.clone(), reason),
            )
            .await
            {
                Ok(Ok(mut conn)) => {
                    let elapsed = now.elapsed();
                    {
                        let mut guard = pool.lock();
                        guard.stats.counts.connect_count += 1;
                        guard.stats.counts.connect_time += elapsed;
                        guard.stats.counts.auth_attempts += conn.password_attempts();
                    }
                    conn.apply_lifetime_jitter(max_age, max_age_jitter);
                    return Ok(conn);
                }

                Ok(Err(err)) => {
                    // We tried all passwords and they were all wrong.
                    if err.is_auth() {
                        pool.lock().stats.counts.auth_attempts += pool.addr().passwords.len();
                        error!(
                            "{}error connecting to server: {} [{}]",
                            if attempt > 0 {
                                format!("[attempt {}] ", attempt)
                            } else {
                                String::new()
                            },
                            err,
                            pool.addr(),
                        );
                        return Err(Error::Auth);
                    } else {
                        error = Error::ServerError;
                    }
                    error!(
                        "{}error connecting to server: {} [{}]",
                        if attempt > 0 {
                            format!("[attempt {}] ", attempt)
                        } else {
                            String::new()
                        },
                        err,
                        pool.addr(),
                    );
                }

                Err(_) => {
                    error!(
                        "{}server connection timeout [{}]",
                        if attempt > 0 {
                            format!("[attempt {}] ", attempt)
                        } else {
                            String::new()
                        },
                        pool.addr(),
                    );
                    error = Error::ConnectTimeout;
                }
            }

            sleep(connect_attempt_delay).await;
        }

        Err(error)
    }
}

#[cfg(test)]
mod test {
    use crate::backend::pool::test::pool;
    use crate::backend::pool::{Address, Config, PoolConfig};

    use super::*;

    #[tokio::test]
    async fn test_healthcheck() {
        crate::logger();
        let pool = pool();
        let ok = Monitor::healthcheck(&pool).await.unwrap();
        assert!(ok);
    }

    #[tokio::test]
    async fn test_healthcheck_sets_health_true_on_success() {
        crate::logger();
        let pool = pool();

        pool.inner().health.toggle(false);
        assert!(!pool.inner().health.healthy());

        let result = Monitor::healthcheck(&pool).await.unwrap();

        assert!(result);
        assert!(pool.inner().health.healthy());
    }

    #[tokio::test]
    async fn test_healthcheck_sets_health_false_on_offline_pool() {
        crate::logger();
        let pool = pool();

        pool.inner().health.toggle(true);
        assert!(pool.inner().health.healthy());

        pool.shutdown();

        let result = Monitor::healthcheck(&pool).await.unwrap();

        assert!(!result);
        assert!(!pool.inner().health.healthy());
    }

    #[tokio::test]
    async fn test_healthcheck_sets_health_false_on_connection_error() {
        crate::logger();

        let config = Config {
            inner: pgdog_stats::Config {
                max: 1,
                min: 1,
                healthcheck_timeout: Duration::from_millis(10),
                ..Config::default().inner
            },
        };

        let pool = Pool::new(&PoolConfig {
            address: Address {
                host: "127.0.0.1".into(),
                port: 1,
                database_name: "pgdog".into(),
                user: "pgdog".into(),
                passwords: vec!["pgdog".into()],
                ..Default::default()
            },
            config,
        });
        pool.launch();

        pool.inner().health.toggle(true);
        assert!(pool.inner().health.healthy());

        let result = Monitor::healthcheck(&pool).await;

        assert!(result.is_err());
        assert!(!pool.inner().health.healthy());
    }

    #[tokio::test]
    async fn test_replenish_only_when_pool_is_online() {
        crate::logger();

        let config = Config {
            inner: pgdog_stats::Config {
                max: 5,
                min: 2,
                ..Config::default().inner
            },
        };

        let pool = Pool::new(&PoolConfig {
            address: Address {
                host: "127.0.0.1".into(),
                port: 5432,
                database_name: "pgdog".into(),
                user: "pgdog".into(),
                passwords: vec!["pgdog".into()],
                ..Default::default()
            },
            config,
        });
        pool.launch();

        let initial_total = pool.lock().total();

        pool.shutdown();
        assert!(!pool.lock().online);

        let monitor = Monitor { pool: pool.clone() };
        let ok = monitor.replenish(ConnectReason::Other).await.unwrap();

        assert!(ok);
        assert_eq!(pool.lock().total(), initial_total);
        assert!(!pool.lock().online);
    }
}
