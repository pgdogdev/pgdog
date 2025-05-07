//! Connection guard.

use std::ops::{Deref, DerefMut};

use tokio::time::timeout;
use tokio::{spawn, time::Instant};
use tracing::{debug, error};

use crate::backend::Server;

use super::{cleanup::Cleanup, Pool};

/// Connection guard.
pub struct Guard {
    server: Option<Box<Server>>,
    pub(super) pool: Pool,
    pub(super) reset: bool,
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field(
                "connected",
                if self.server.is_some() {
                    &"true"
                } else {
                    &"false"
                },
            )
            .finish()
    }
}

impl Guard {
    /// Create new connection guard.
    pub fn new(pool: Pool, mut server: Box<Server>, granted_at: Instant) -> Self {
        server.stats_mut().set_timers(granted_at);

        Self {
            server: Some(server),
            pool,
            reset: false,
        }
    }

    /// Rollback any unfinished transactions and check the connection
    /// back into the pool.
    fn cleanup(&mut self) {
        let server = self.server.take();
        let pool = self.pool.clone();

        if let Some(mut server) = server {
            let rollback = server.in_transaction();
            let cleanup = Cleanup::new(self, &server);
            let reset = cleanup.needed();
            let schema_changed = server.schema_changed();
            let sync_prepared = server.sync_prepared();
            let receiving_data = server.receiving_data();

            server.reset_changed_params();

            // No need to delay checkin unless we have to.
            if rollback || reset || sync_prepared || receiving_data {
                let rollback_timeout = pool.lock().config.rollback_timeout();
                spawn(async move {
                    if receiving_data {
                        // Receive whatever data the client left before disconnecting.
                        server.drain().await;
                    }
                    // Rollback any unfinished transactions,
                    // but only if the server is in sync (protocol-wise).
                    if rollback {
                        if timeout(rollback_timeout, server.rollback()).await.is_err() {
                            error!("rollback timeout [{}]", server.addr());
                        }
                    }

                    if cleanup.needed() {
                        match timeout(rollback_timeout, server.execute_batch(cleanup.queries()))
                            .await
                        {
                            Ok(Err(_)) | Err(_) => {
                                error!("server reset error [{}]", server.addr());
                            }
                            Ok(Ok(_)) => {
                                debug!("{} [{}]", cleanup, server.addr());
                                server.cleaned();
                            }
                        }
                    }

                    if schema_changed {
                        server.reset_schema_changed();
                    }

                    if cleanup.is_reset_params() {
                        server.reset_params();
                    }

                    if sync_prepared {
                        if let Err(err) = server.sync_prepared_statements().await {
                            error!(
                                "prepared statements sync error: {:?} [{}]",
                                err,
                                server.addr()
                            );
                        }
                    }

                    pool.checkin(server);
                });
            } else {
                pool.checkin(server);
            }
        }
    }
}

impl Deref for Guard {
    type Target = Server;

    fn deref(&self) -> &Self::Target {
        self.server.as_ref().unwrap()
    }
}

impl DerefMut for Guard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.server.as_mut().unwrap()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.cleanup();
    }
}
