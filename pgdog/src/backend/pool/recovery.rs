//! Recover server connections and return them to their pool.

use pgdog_config::pooling::ConnectionRecovery;
use tokio::spawn;
use tracing::{debug, error};

use crate::backend::{Error, Server};
use crate::util::safe_timeout;

use super::{Pool, cleanup::Cleanup};

/// Own a checked-out server until recovery and pool check-in finish.
pub(super) struct Recovery {
    server: Box<Server>,
    pool: Pool,
    cleanup: Cleanup,
    needed: bool,
}

impl Recovery {
    pub(super) fn new(mut server: Box<Server>, pool: Pool, cleanup: Cleanup) -> Self {
        let needed = (server.in_transaction()
            || cleanup.needed()
            || server.sync_prepared()
            || server.needs_drain())
            && !server.is_force_close();
        server.reset_changed_params();

        Self {
            server,
            pool,
            cleanup,
            needed,
        }
    }

    /// Check in immediately when possible, otherwise recover in the background.
    pub(super) fn recover(self) {
        let Self {
            mut server,
            pool,
            cleanup,
            needed,
        } = self;

        if needed {
            let rollback_timeout = pool.inner().config.rollback_timeout;
            let conn_recovery = pool.inner().config.connection_recovery;

            spawn(async move {
                match safe_timeout(
                    rollback_timeout,
                    Self::cleanup_internal(&mut server, cleanup, conn_recovery),
                )
                .await
                {
                    Ok(Ok(_)) => (),
                    Err(_) => {
                        error!("server cleanup timed out [{}]", server.addr());
                        server.force_close();
                    }
                    Ok(Err(err)) => {
                        error!("server cleanup failed: {} [{}]", err, server.addr());
                        if !server.error() {
                            server.force_close();
                        }
                    }
                }

                Self::checkin(&pool, server);
            });
        } else {
            debug!(
                "[cleanup] no cleanup needed, server in \"{}\" state [{}]",
                server.stats().get_state(),
                server.addr(),
            );
            Self::checkin(&pool, server);
        }
    }

    fn checkin(pool: &Pool, server: Box<Server>) {
        if let Err(err) = pool.checkin(server) {
            error!("pool checkin error: {} [{}]", err, pool.addr());
        }
    }

    /// Make sure the connection is protocol-synchronized.
    async fn resynchronize(
        server: &mut Server,
        recovery: ConnectionRecovery,
    ) -> Result<bool, Error> {
        if server.needs_drain() {
            if !server.has_more_messages() {
                server.synchronize().await?;
            } else if recovery.can_recover() && !server.is_sending_request() {
                debug!(
                    "[cleanup] draining data from \"{}\" server [{}]",
                    server.stats().get_state(),
                    server.addr()
                );

                server.drain().await?;
            } else {
                server.force_close();
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Rollback unfinished transaction, if any is started.
    ///
    /// Caller must ensure the server connection is synchronized, e.g., with [`Self::resynchronize`].
    async fn rollback_transaction(
        server: &mut Server,
        recovery: ConnectionRecovery,
    ) -> Result<bool, Error> {
        // Rollback any unfinished transactions,
        // but only if the server is in sync (protocol-wise).
        if server.in_transaction() {
            if recovery.can_rollback() {
                debug!(
                    "[cleanup] rolling back server transaction, in \"{}\" state [{}]",
                    server.stats().get_state(),
                    server.addr(),
                );
                server.rollback().await?;
            } else {
                server.force_close();
                return Ok(false);
            }
        }

        Ok(true)
    }

    // Reset server parameters if the server was pinned,
    // and evict prepared statements exceeding the cache size.
    async fn sync_params_and_prepared_statements(
        server: &mut Server,
        cleanup: &Cleanup,
    ) -> Result<(), Error> {
        if cleanup.needed() {
            debug!(
                "[cleanup] running {} cleanup queries, server in \"{}\" state [{}]",
                cleanup.len(),
                server.stats().get_state(),
                server.addr()
            );
            server.execute_batch(cleanup.queries()).await?;

            if cleanup.is_deallocate() {
                server.prepared_statements_mut().clear();
            }
            server.cleaned();

            debug!(
                "[cleanup] closing {} prepared statements",
                cleanup.close().len()
            );
            server.close_many(cleanup.close()).await?;
        }

        Ok(())
    }

    // Reset server state flags.
    fn cleanup_state(server: &mut Server, cleanup: &Cleanup) {
        if server.schema_changed() {
            server.reset_schema_changed();
        }

        if cleanup.is_reset_params() {
            server.reset_params();
        }
    }

    async fn load_server_prepared_statements(server: &mut Server) -> Result<(), Error> {
        if server.sync_prepared() {
            debug!(
                "[cleanup] syncing prepared statements, server in \"{}\" state [{}]",
                server.stats().get_state(),
                server.addr()
            );
            server.sync_prepared_statements().await?;
        }

        Ok(())
    }

    pub(super) async fn cleanup_internal(
        server: &mut Box<Server>,
        cleanup: Cleanup,
        conn_recovery: ConnectionRecovery,
    ) -> Result<(), Error> {
        if !Self::resynchronize(server, conn_recovery).await? {
            return Ok(());
        }

        if !Self::rollback_transaction(server, conn_recovery).await? {
            return Ok(());
        }

        Self::sync_params_and_prepared_statements(server, &cleanup).await?;
        Self::cleanup_state(server, &cleanup);
        Self::load_server_prepared_statements(server).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use pgdog_config::pooling::ConnectionRecovery;

    use crate::backend::pool::{Address, Config, Pool, PoolConfig, Request};
    use crate::net::{Flush, Parse, Protocol, Query};

    const RECOVERY_MODES: [ConnectionRecovery; 3] = [
        ConnectionRecovery::Recover,
        ConnectionRecovery::RollbackOnly,
        ConnectionRecovery::Drop,
    ];

    fn pool(recovery: ConnectionRecovery) -> Pool {
        let pool = Pool::new(&PoolConfig {
            address: Address::new_test(),
            config: Config {
                max: 1,
                min: 0,
                checkout_timeout: Duration::from_secs(5),
                connection_recovery: recovery,
                ..Config::default()
            },
        });
        pool.launch();
        pool
    }

    #[tokio::test]
    async fn test_resynchronize_after_flush_reuses_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        for recovery in RECOVERY_MODES {
            let pool = pool(recovery);
            let mut guard = pool.get(&Request::default()).await?;
            let id = guard.id();

            guard
                .send(&vec![Parse::named("test", "SELECT 1").into(), Flush.into()].into())
                .await?;
            assert_eq!(guard.read().await?.code(), '1');
            assert!(guard.needs_drain());
            assert!(!guard.has_more_messages());
            assert!(!guard.in_transaction());
            drop(guard);

            let mut guard = pool.get(&Request::default()).await?;
            assert_eq!(guard.id(), id, "recovery mode: {recovery:?}");
            assert!(guard.in_sync());
            assert!(!guard.has_more_messages());
            assert!(!guard.is_force_close());
            let state = pool.state();
            assert_eq!(state.re_synced, 1);
            assert_eq!(state.force_close, 0);
            assert_eq!(state.errors, 0);
            let rows: Vec<i32> = guard.fetch_all("SELECT 1").await?;
            assert_eq!(rows, vec![1]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_resynchronize_after_flush_respects_rollback_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        for recovery in RECOVERY_MODES {
            let pool = pool(recovery);
            let mut guard = pool.get(&Request::default()).await?;
            let id = guard.id();
            guard.execute("BEGIN").await?;
            guard
                .send(&vec![Parse::named("test", "SELECT 1").into(), Flush.into()].into())
                .await?;
            assert_eq!(guard.read().await?.code(), '1');
            assert!(guard.needs_drain());
            assert!(!guard.has_more_messages());
            assert!(guard.in_transaction());
            drop(guard);

            let mut guard = pool.get(&Request::default()).await?;
            match recovery {
                ConnectionRecovery::Recover | ConnectionRecovery::RollbackOnly => {
                    assert_eq!(guard.id(), id, "recovery mode: {recovery:?}");
                    assert_eq!(pool.state().force_close, 0);
                    assert_eq!(pool.state().re_synced, 1);
                }
                ConnectionRecovery::Drop => {
                    assert_ne!(guard.id(), id);
                    assert_eq!(pool.state().force_close, 1);
                }
            }
            assert!(guard.in_sync());
            assert!(!guard.in_transaction());
            assert_eq!(pool.state().errors, 0);
            let rows: Vec<i32> = guard.fetch_all("SELECT 1").await?;
            assert_eq!(rows, vec![1]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_resynchronize_with_unread_responses_respects_recovery_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        for recovery in RECOVERY_MODES {
            let pool = pool(recovery);
            let mut guard = pool.get(&Request::default()).await?;
            let id = guard.id();
            guard
                .send(&vec![Parse::named("test", "SELECT 1").into(), Flush.into()].into())
                .await?;
            assert!(guard.needs_drain());
            assert!(guard.has_more_messages());
            drop(guard);

            let mut guard = pool.get(&Request::default()).await?;
            match recovery {
                ConnectionRecovery::Recover => {
                    assert_eq!(guard.id(), id);
                    assert_eq!(pool.state().force_close, 0);
                    assert_eq!(pool.state().re_synced, 1);
                }
                ConnectionRecovery::RollbackOnly | ConnectionRecovery::Drop => {
                    assert_ne!(guard.id(), id, "recovery mode: {recovery:?}");
                    assert_eq!(pool.state().force_close, 1);
                    assert_eq!(pool.state().re_synced, 0);
                }
            }
            assert!(guard.in_sync());
            assert!(!guard.has_more_messages());
            assert_eq!(pool.state().errors, 0);
            let rows: Vec<i32> = guard.fetch_all("SELECT 1").await?;
            assert_eq!(rows, vec![1]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_draining_ready_for_query_does_not_count_as_resynchronization()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = pool(ConnectionRecovery::Recover);
        let mut guard = pool.get(&Request::default()).await?;
        let id = guard.id();
        guard
            .send(&vec![Query::new("SELECT 1").into()].into())
            .await?;
        assert!(guard.needs_drain());
        assert!(guard.has_more_messages());
        drop(guard);

        let mut guard = pool.get(&Request::default()).await?;
        assert_eq!(guard.id(), id);
        assert!(guard.in_sync());
        assert!(!guard.has_more_messages());
        let state = pool.state();
        assert_eq!(state.re_synced, 0);
        assert_eq!(state.force_close, 0);
        assert_eq!(state.errors, 0);
        let rows: Vec<i32> = guard.fetch_all("SELECT 1").await?;
        assert_eq!(rows, vec![1]);

        Ok(())
    }
}
