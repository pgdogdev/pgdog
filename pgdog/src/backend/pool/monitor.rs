//! Pool monitor and maintenance.

use std::time::{Duration, Instant};

use super::{Error, Guard, Healtcheck, Pool};
use crate::backend::Server;

use tokio::time::{interval, sleep, timeout};
use tokio::{select, task::spawn};
use tracing::info;

use tracing::{debug, error};

/// Pool maintenance.
pub struct Monitor {
    pool: Pool,
}

impl Monitor {
    /// Launch the pool maintenance loop.
    pub fn new(pool: &Pool) {
        let monitor = Self { pool: pool.clone() };

        spawn(async move {
            monitor.spawn().await;
        });
    }

    /// Run the connection pool.
    async fn spawn(self) {
        debug!("maintenance loop is running [{}]", self.pool.addr());

        let pool = self.pool.clone();
        spawn(async move { Self::maintenance(pool).await });
        let pool = self.pool.clone();
        let delay = { pool.lock().config().idle_healtcheck_delay() };
        spawn(async move {
            sleep(delay).await;
            Self::healthchecks(pool).await
        });

        loop {
            let comms = self.pool.comms();

            select! {
                // A client is requesting a connection and no idle
                // connections are availble.
                _ = comms.request.notified() => {
                    let (
                        empty,
                        can_create,
                        connect_timeout,
                        paused,
                        banned,
                        online,
                    ) = {
                        let guard = self.pool.lock();

                        (
                            guard.empty(),
                            guard.can_create(),
                            guard.config().connect_timeout(),
                            guard.paused,
                            guard.banned(),
                            guard.online,
                        )
                    };

                    if !online {
                        break;
                    }

                    // If the pool is paused, don't open new connections.
                    if paused {
                        continue;
                    }

                    // An idle connection is available.
                    if !empty {
                        comms.ready.notify_one();
                    } else if can_create && !banned {
                        // No idle connections, but we are allowed to create a new one.
                        let ok = self.replenish(connect_timeout).await;

                        if ok {
                            comms.ready.notify_one();
                        }
                    }
                }

                // Pool is shutting down.
                _ = comms.shutdown.notified() => {
                    self.pool.lock().online = false;
                    break;
                }
            }
        }

        debug!("maintenance loop is shut down [{}]", self.pool.addr());
    }

    async fn healthchecks(pool: Pool) {
        let mut tick = interval(pool.lock().config().idle_healthcheck_interval());
        let comms = pool.comms();

        debug!("healtchecks running [{}]", pool.addr());

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

                    if Self::healthcheck(&pool).await.is_ok() {
                        let mut guard = pool.lock();
                        unbanned = guard.maybe_unban();
                    }
                }


                _ = comms.shutdown.notified() => break,
            }

            if unbanned {
                info!("pool unbanned [{}]", pool.addr());
            }
        }

        debug!("healthchecks stopped [{}]", pool.addr());
    }

    /// Perform maintenance on the pool periodically.
    async fn maintenance(pool: Pool) {
        let maintenance_interval = if pool.lock().banned() {
            Duration::from_secs(1)
        } else {
            Duration::from_millis(333)
        };

        let mut tick = interval(maintenance_interval);
        let comms = pool.comms();

        debug!("maintenance started [{}]", pool.addr());

        loop {
            select! {
                _ = tick.tick() => {
                    let now = Instant::now();

                    let mut guard = pool.lock();

                    if !guard.online {
                        break;
                    }

                    guard.close_idle(now);
                    guard.close_old(now);
                    let unbanned = guard.check_ban(now);

                    // If we have clients waiting still, try to open a connection again.
                    // This prevents a thundering herd.
                    if guard.waiting > 0 {
                        comms.request.notify_one();
                    }

                    if guard.should_create() {
                        comms.request.notify_one();
                    }

                    if unbanned {
                        info!("pool unbanned [{}]", pool.addr());
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

        match timeout(connect_timeout, Server::connect(self.pool.addr())).await {
            Ok(Ok(conn)) => {
                ok = true;
                self.pool.lock().conns.push_front(conn);
            }

            Ok(Err(err)) => {
                self.pool.ban(Error::ServerError);
                error!("error connecting to server: {} [{}]", err, self.pool.addr());
            }

            Err(_) => {
                self.pool.ban(Error::ServerError);
                error!("server connection timeout [{}]", self.pool.addr());
            }
        }

        ok
    }

    /// Perform a periodic healthcheck on the pool.
    async fn healthcheck(pool: &Pool) -> Result<(), Error> {
        let (conn, healthcheck_timeout) = {
            let mut guard = pool.lock();
            if !guard.online {
                return Ok(());
            }
            (guard.conns.pop_front(), guard.config.healthcheck_timeout())
        };

        // Have an idle connection, use that for the healtcheck.
        if let Some(conn) = conn {
            Healtcheck::mandatory(
                Guard::new(pool.clone(), conn),
                pool.clone(),
                healthcheck_timeout,
            )
            .healtcheck()
            .await?;

            Ok(())
        } else {
            // Create a new one and close it. once done.
            debug!("creating new healthcheck connection [{}]", pool.addr());
            match Server::connect(pool.addr()).await {
                Ok(mut server) => {
                    if let Ok(()) = server.healthcheck(";").await {
                        return Ok(());
                    }
                }

                Err(err) => {
                    error!("healthcheck error: {} [{}]", err, pool.addr());
                }
            }

            Err(Error::HealtcheckError)
        }
    }
}
