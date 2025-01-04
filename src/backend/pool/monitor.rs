//! Pool monitor and maintenance.

use std::time::{Duration, Instant};

use super::Pool;
use crate::backend::{Error, Server};

use tokio::time::{sleep, timeout};
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

        loop {
            let comms = self.pool.comms();

            // If the pool is banned, don't try to create new connections
            // more often than once a second. Otherwise, perform maintenance
            // on the pool ~3 times per second.
            let maintenance_interval = if self.pool.lock().banned() {
                Duration::from_secs(1)
            } else {
                Duration::from_millis(333)
            };

            let mut unbanned = false;

            select! {
                // A client is requesting a connection and no idle
                // connections are availble.
                _ = comms.request.notified() => {
                    let (
                        empty,
                        can_create,
                        connect_timeout,
                        paused,
                        _banned,
                    ) = {
                        let guard = self.pool.lock();

                        (
                            guard.empty(),
                            guard.can_create(),
                            guard.config().connect_timeout(),
                            guard.paused,
                            guard.banned(),
                        )
                    };

                    // If the pool is paused, don't open new connections.
                    if paused {
                        continue;
                    }

                    // An idle connection is available.
                    if !empty {
                        comms.ready.notify_one();
                    } else if can_create {
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

                // Perform maintenance.
                _ = sleep(maintenance_interval) => {
                    let now = Instant::now();

                    let mut guard = self.pool.lock();

                    guard.close_idle(now);
                    guard.close_old(now);
                    unbanned = guard.check_ban(now);

                    // If we have clients waiting still, try to open a connection again.
                    // This prevents a thundering herd.
                    if guard.waiting > 0 {
                        comms.request.notify_one();
                    }

                    if guard.should_create() {
                        comms.request.notify_one();
                    }
                }
            }

            if unbanned {
                info!("pool unbanned [{}]", self.pool.addr());
            }
        }

        debug!("maintenance loop is shut down [{}]", self.pool.addr());
    }

    async fn replenish(&self, connect_timeout: Duration) -> bool {
        let mut ok = false;

        match timeout(connect_timeout, Server::connect(self.pool.addr())).await {
            Ok(Ok(conn)) => {
                ok = true;
                self.pool.lock().conns.push_front(conn);
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
}
