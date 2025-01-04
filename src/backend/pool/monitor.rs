//! Pool monitor and maintenance.

use std::time::{Duration, Instant};

use super::Pool;
use crate::backend::Server;

use tokio::time::{sleep, timeout};
use tokio::{select, task::spawn};

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
        debug!("Maintenance loop is running [{}]", self.pool.addr());

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
                    ) = {
                        let guard = self.pool.lock();

                        (
                            guard.empty(),
                            guard.can_create(),
                            guard.config().connect_timeout(),
                            guard.paused,
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

                        match timeout(connect_timeout, Server::connect(self.pool.addr())).await {
                            Ok(Ok(conn)) => {
                                let mut guard = self.pool.lock();

                                guard.conns.push_front(conn);
                                comms.ready.notify_one();
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

                // Pool is shutting down.
                _ = comms.shutdown.notified() => {
                    self.pool.lock().online = false;
                    break;
                }

                // Perform maintenance ~3 times per second.
                _ = sleep(Duration::from_millis(333)) => {
                    let now = Instant::now();

                    let mut guard = self.pool.lock();

                    guard.close_idle(now);
                    guard.close_old(now);
                    guard.check_ban(now);

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
        }

        debug!("Maintenance loop is shut down [{}]", self.pool.addr());
    }
}
