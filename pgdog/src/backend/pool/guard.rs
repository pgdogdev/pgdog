//! Connection guard.

use std::ops::{Deref, DerefMut};

use tokio::spawn;
use tokio::time::timeout;
use tracing::error;

use crate::backend::Server;

use super::Pool;

/// Connection guard.
pub struct Guard {
    server: Option<Server>,
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
    pub fn new(pool: Pool, server: Server) -> Self {
        Self {
            server: Some(server),
            pool,
            reset: false,
        }
    }

    /// Rollback any unfinished transactions and check the connection
    /// back into the pool.
    fn rollback(&mut self) {
        let server = self.server.take();
        let pool = self.pool.clone();

        if let Some(mut server) = server {
            let rollback = server.in_transaction();
            let reset = self.reset;

            if rollback || reset {
                let rollback_timeout = pool.lock().config.rollback_timeout();
                spawn(async move {
                    // Rollback any unfinished transactions,
                    // but only if the server is in sync (protocol-wise).
                    if rollback {
                        if let Err(_) = timeout(rollback_timeout, server.rollback()).await {
                            error!("rollback timeout [{}]", server.addr());
                        }
                    }

                    if reset {
                        if let Err(_) = timeout(rollback_timeout, server.reset()).await {
                            error!("reset timeout [{}]", server.addr());
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
        self.rollback();
    }
}
