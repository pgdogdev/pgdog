//! Connection guard.

use std::ops::{Deref, DerefMut};

use tokio::time::timeout;
use tokio::{spawn, time::Instant};
use tracing::{debug, error};

use crate::backend::Server;
use crate::state::State;

use super::Error;
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
            let cleanup = Cleanup::new(self, &mut server);
            let reset = cleanup.needed();
            let sync_prepared = server.sync_prepared();
            let needs_drain = server.needs_drain();
            let force_close = server.force_close();

            server.reset_changed_params();

            // No need to delay checkin unless we have to.
            if (rollback || reset || sync_prepared || needs_drain) && !force_close {
                let rollback_timeout = pool.inner().config.rollback_timeout();
                spawn(async move {
                    if timeout(
                        rollback_timeout,
                        Self::cleanup_internal(&mut server, cleanup),
                    )
                    .await
                    .is_err()
                    {
                        error!("rollback timeout [{}]", server.addr());
                    };

                    pool.checkin(server);
                });
            } else {
                debug!(
                    "[cleanup] no cleanup needed, server in \"{}\" state [{}]",
                    server.stats().state,
                    server.addr(),
                );
                pool.checkin(server);
            }
        }
    }

    async fn cleanup_internal(server: &mut Box<Server>, cleanup: Cleanup) -> Result<(), Error> {
        let schema_changed = server.schema_changed();
        let sync_prepared = server.sync_prepared();
        let needs_drain = server.needs_drain();

        if needs_drain {
            // Receive whatever data the client left before disconnecting.
            debug!(
                "[cleanup] draining data from \"{}\" server [{}]",
                server.stats().state,
                server.addr()
            );
            server.drain().await;
        }
        let rollback = server.in_transaction();

        // Rollback any unfinished transactions,
        // but only if the server is in sync (protocol-wise).
        if rollback {
            debug!(
                "[cleanup] rolling back server transaction, in \"{}\" state [{}]",
                server.stats().state,
                server.addr(),
            );
            server.rollback().await;
        }

        if cleanup.needed() {
            debug!(
                "[cleanup] {}, server in \"{}\" state [{}]",
                cleanup,
                server.stats().state,
                server.addr()
            );
            match server.execute_batch(cleanup.queries()).await {
                Err(_) => {
                    error!("server reset error [{}]", server.addr());
                }
                Ok(_) => {
                    if cleanup.is_deallocate() {
                        server.prepared_statements_mut().clear();
                    }
                    server.cleaned();
                }
            }

            match server.close_many(cleanup.close()).await {
                Ok(_) => (),
                Err(err) => {
                    server.stats_mut().state(State::Error);
                    error!("server close error: {} [{}]", err, server.addr());
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
            debug!(
                "[cleanup] syncing prepared statements, server in \"{}\" state [{}]",
                server.stats().state,
                server.addr()
            );
            if let Err(err) = server.sync_prepared_statements().await {
                error!(
                    "prepared statements sync error: {:?} [{}]",
                    err,
                    server.addr()
                );
            }
        }

        Ok(())
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

#[cfg(test)]
mod test {
    use crate::{
        backend::pool::{test::pool, Request},
        net::{Describe, Flush, Parse, Protocol, Query, Sync},
    };

    #[tokio::test]
    async fn test_cleanup_dirty() {
        crate::logger();
        let pool = pool();
        let mut guard = pool.get(&Request::default()).await.unwrap();

        guard
            .send(&vec![Parse::named("test", "SELECT $1").into(), Flush.into()].into())
            .await
            .unwrap();
        let msg = guard.read().await.unwrap();
        assert_eq!(msg.code(), '1');
        assert!(guard.done());

        guard
            .send(&vec![Query::new("SELECT pg_advisory_lock(123456)").into()].into())
            .await
            .unwrap();

        for c in ['T', 'D', 'C', 'Z'] {
            let msg = guard.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert!(guard.done());

        guard.mark_dirty(true);
        drop(guard);

        // Our test pool is only 1 connection.
        //
        let mut guard = pool.get(&Request::default()).await.unwrap();

        guard
            .send(&vec![Describe::new_statement("test").into(), Sync.into()].into())
            .await
            .unwrap();

        for code in ['t', 'T', 'Z'] {
            let msg = guard.read().await.unwrap();
            assert_eq!(msg.code(), code);
        }

        // Try to lock again, should work.
        guard
            .send(&vec![Query::new("SELECT pg_advisory_lock(123456)").into()].into())
            .await
            .unwrap();

        for c in ['T', 'D', 'C', 'Z'] {
            let msg = guard.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert!(guard.done());

        guard.mark_dirty(true);
        drop(guard);
    }

    #[tokio::test]
    async fn test_cleanup_prepared_statements() {
        crate::logger();
        let pool = pool();
        let mut guard = pool.get(&Request::default()).await.unwrap();

        guard
            .send(&vec![Parse::named("test", "SELECT $1").into(), Flush.into()].into())
            .await
            .unwrap();
        let msg = guard.read().await.unwrap();
        assert_eq!(msg.code(), '1');
        assert!(guard.done());

        assert_eq!(guard.prepared_statements().len(), 1);
        guard.reset = true;
        drop(guard);

        let guard = pool.get(&Request::default()).await.unwrap();
        assert!(guard.prepared_statements().is_empty());
    }
}
