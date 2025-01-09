//! Connection guard.

use std::ops::{Deref, DerefMut};

use tokio::spawn;

use crate::backend::Server;

use super::Pool;

/// Connection guard.
pub struct Guard {
    server: Option<Server>,
    pool: Pool,
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
        }
    }

    /// Rollback any unfinished transactions and check the connection
    /// back into the pool.
    fn rollback(&mut self) {
        let server = self.server.take();
        let pool = self.pool.clone();

        if let Some(mut server) = server {
            spawn(async move {
                server.rollback().await;
                pool.checkin(server);
            });
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
