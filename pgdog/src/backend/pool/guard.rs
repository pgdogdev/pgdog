//! Connection guard.

use std::ops::{Deref, DerefMut};

use tokio::time::Instant;

use crate::backend::Server;

use super::{Pool, cleanup::Cleanup, recovery::Recovery};

/// Connection guard.
pub(crate) struct Guard {
    server: Option<Box<Server>>,
    pub(super) pool: Pool,
    pub(super) reset: bool,

    /// Frontend has pinned this guard to its client
    // NOTE: We cache this value seperately to minimize pool lock contention
    locked: bool,
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
    pub(crate) fn new(pool: Pool, mut server: Box<Server>, granted_at: Instant) -> Self {
        server.stats_mut().set_timers(granted_at);

        Self {
            server: Some(server),
            pool,
            reset: false,
            locked: false,
        }
    }

    /// Mark or unmark this checkout as pinned to its client. Propagates to
    /// the pool so `sv_locked` reflects reality per pool.
    pub(crate) fn set_locked(&mut self, locked: bool) {
        if self.locked == locked {
            // No-op, don't bother aquiring a lock
            return;
        }

        self.locked = locked;
        if let Some(server) = self.server.as_deref() {
            self.pool.set_locked(server.id(), locked);
        }
    }

    /// Whether this guard is currently pinned to its client.
    #[inline]
    pub(crate) fn is_locked(&self) -> bool {
        self.locked
    }

    /// Rollback any unfinished transactions and check the connection
    /// back into the pool.
    fn cleanup(&mut self) {
        if let Some(mut server) = self.server.take() {
            let cleanup = Cleanup::new(self, &mut server);
            Recovery::new(server, self.pool.clone(), cleanup).recover();
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use pgdog_config::pooling::ConnectionRecovery;
    use pgdog_config::prepared_statements::PreparedStatementsConfig;
    use tokio::time::Instant;

    use crate::util::{safe_sleep, safe_timeout};
    use crate::{
        backend::{
            pool::{
                Address, Config, Guard, Pool, PoolConfig, Request, cleanup::Cleanup,
                recovery::Recovery, test::pool,
            },
            server::test::test_server,
        },
        net::{DataRow, Describe, Flush, Parse, Protocol, ProtocolMessage, Query, Sync},
    };

    async fn identity(guard: &mut Guard) -> (String, String) {
        let messages = guard
            .execute("SELECT session_user, current_user")
            .await
            .unwrap();
        let row: DataRow = messages
            .into_iter()
            .find(|message| message.code() == 'D')
            .expect("identity query should return one row")
            .try_into()
            .unwrap();

        (
            row.get_text(0).expect("session_user"),
            row.get_text(1).expect("current_user"),
        )
    }

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
    async fn test_cleanup_dirty_resets_session_identity() {
        crate::logger();
        let pool = pool();
        let mut guard = pool.get(&Request::default()).await.unwrap();
        let server_id = guard.id();
        let role = format!("pgdog_cleanup_role_{}", std::process::id());

        guard
            .execute_checked(format!("CREATE ROLE {role}"))
            .await
            .unwrap();
        guard
            .execute_checked(format!("SET ROLE {role}"))
            .await
            .unwrap();
        assert_eq!(identity(&mut guard).await, ("pgdog".into(), role.clone()));

        guard.mark_dirty(true);
        drop(guard);

        let mut guard = pool.get(&Request::default()).await.unwrap();
        assert_eq!(guard.id(), server_id);
        assert_eq!(identity(&mut guard).await, ("pgdog".into(), "pgdog".into()));

        guard
            .execute_checked(format!("SET SESSION AUTHORIZATION {role}"))
            .await
            .unwrap();
        assert_eq!(identity(&mut guard).await, (role.clone(), role.clone()));

        guard.mark_dirty(true);
        drop(guard);

        let mut guard = pool.get(&Request::default()).await.unwrap();
        assert_eq!(guard.id(), server_id);
        assert_eq!(identity(&mut guard).await, ("pgdog".into(), "pgdog".into()));
        guard
            .execute_checked(format!("DROP ROLE {role}"))
            .await
            .unwrap();
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
        assert_eq!(guard.prepared_statements().len(), 0);
    }

    #[tokio::test]
    async fn test_rollback_timeout() {
        crate::logger();

        let config = Config {
            max: 1,
            min: 0,
            rollback_timeout: Duration::from_millis(100),
            ..Config::default()
        };

        let pool = Pool::new(&PoolConfig {
            address: Address::new_test(),
            config,
        });
        pool.launch();

        {
            let mut guard = pool.get(&Request::default()).await.unwrap();

            guard.execute("BEGIN").await.unwrap();
            assert!(guard.in_transaction());

            guard
                .send(&vec![Query::new("SELECT pg_sleep(1)").into()].into())
                .await
                .unwrap();
        }

        safe_sleep(Duration::from_millis(500)).await;

        {
            let state = pool.lock();
            assert_eq!(state.errors, 0);
            assert_eq!(state.idle(), 0);
            assert_eq!(state.total(), 0);
            assert_eq!(state.force_close, 1);
        }

        // Will create new connection.
        let mut server = pool.get(&Request::default()).await.unwrap();
        let one: Vec<i32> = server.fetch_all("SELECT 1").await.unwrap();
        assert_eq!(one[0], 1);
    }

    #[tokio::test]
    async fn test_cleanup_close_drain() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );
        server
            .prepared_statements_mut()
            .configure(PreparedStatementsConfig {
                limit: 1,
                ..Default::default()
            });

        for i in 0..5 {
            server
                .send(
                    &vec![
                        ProtocolMessage::from(Parse::named(format!("test_{}", i), "SELECT 1")),
                        Flush.into(),
                    ]
                    .into(),
                )
                .await
                .unwrap();

            let ok = server.read().await.unwrap();
            assert_eq!(ok.code(), '1');
            assert!(server.done());
        }
        assert_eq!(server.prepared_statements().len(), 5);
        server
            .send(&vec![Query::new("SHOW prepared_statements").into()].into())
            .await
            .unwrap();
        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);
        assert_eq!(cleanup.close().len(), 4);
        assert!(server.needs_drain());

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::Recover)
            .await
            .unwrap();

        assert!(server.done());
        assert!(!server.needs_drain());

        let one: Vec<i32> = server.fetch_all("SELECT 1").await.unwrap();
        assert_eq!(one[0], 1);
    }

    #[tokio::test]
    async fn test_cancel_safety_partial_send() {
        let mut server = test_server().await;
        let select = (0..50_000_000).map(|_| 'b').collect::<String>();
        let select = Query::new(format!("SELECT '{}'", select));
        let res = safe_timeout(
            Duration::from_millis(1),
            server.send(&vec![select.into()].into()),
        )
        .await;
        assert!(res.is_err());
        assert!(server.is_force_close());
        assert!(server.io_in_progress())
    }

    #[tokio::test]
    async fn test_conn_recovery_recover_with_needs_drain() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );
        server
            .prepared_statements_mut()
            .configure(PreparedStatementsConfig {
                limit: 1,
                ..Default::default()
            });

        server
            .send(
                &vec![
                    ProtocolMessage::from(Parse::named("test_0", "SELECT 1")),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        let ok = server.read().await.unwrap();
        assert_eq!(ok.code(), '1');
        assert!(server.done());

        server
            .send(&vec![Query::new("SHOW prepared_statements").into()].into())
            .await
            .unwrap();

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        assert!(server.needs_drain());

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::Recover)
            .await
            .unwrap();

        assert!(server.done());
        assert!(!server.needs_drain());

        let one: Vec<i32> = server.fetch_all("SELECT 1").await.unwrap();
        assert_eq!(one[0], 1);
    }

    #[tokio::test]
    async fn test_conn_recovery_rollback_only_with_needs_drain() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );
        server
            .prepared_statements_mut()
            .configure(PreparedStatementsConfig {
                limit: 1,
                ..Default::default()
            });

        server
            .send(
                &vec![
                    ProtocolMessage::from(Parse::named("test_0", "SELECT 1")),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        let ok = server.read().await.unwrap();
        assert_eq!(ok.code(), '1');
        assert!(server.done());

        server
            .send(&vec![Query::new("SHOW prepared_statements").into()].into())
            .await
            .unwrap();

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        assert!(server.needs_drain());

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::RollbackOnly)
            .await
            .unwrap();

        use crate::state::State;
        assert_eq!(server.stats().get_state(), State::ForceClose);
        assert!(server.needs_drain());
    }

    #[tokio::test]
    async fn test_conn_recovery_drop_with_needs_drain() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );
        server
            .prepared_statements_mut()
            .configure(PreparedStatementsConfig {
                limit: 1,
                ..Default::default()
            });

        server
            .send(
                &vec![
                    ProtocolMessage::from(Parse::named("test_0", "SELECT 1")),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        let ok = server.read().await.unwrap();
        assert_eq!(ok.code(), '1');
        assert!(server.done());

        server
            .send(&vec![Query::new("SHOW prepared_statements").into()].into())
            .await
            .unwrap();

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        assert!(server.needs_drain());

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::Drop)
            .await
            .unwrap();

        use crate::state::State;
        assert_eq!(server.stats().get_state(), State::ForceClose);
        assert!(server.needs_drain());
    }

    #[tokio::test]
    async fn test_conn_recovery_recover_with_rollback() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );

        server
            .send(&vec![Query::new("BEGIN").into()].into())
            .await
            .unwrap();

        loop {
            let msg = server.read().await.unwrap();
            if msg.code() == 'Z' {
                break;
            }
        }

        assert!(server.in_transaction());

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::Recover)
            .await
            .unwrap();

        assert!(!server.in_transaction());

        let one: Vec<i32> = server.fetch_all("SELECT 1").await.unwrap();
        assert_eq!(one[0], 1);
    }

    #[tokio::test]
    async fn test_conn_recovery_rollback_only_with_rollback() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );

        server
            .send(&vec![Query::new("BEGIN").into()].into())
            .await
            .unwrap();

        loop {
            let msg = server.read().await.unwrap();
            if msg.code() == 'Z' {
                break;
            }
        }

        assert!(server.in_transaction());

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::RollbackOnly)
            .await
            .unwrap();

        assert!(!server.in_transaction());

        let one: Vec<i32> = server.fetch_all("SELECT 1").await.unwrap();
        assert_eq!(one[0], 1);
    }

    #[tokio::test]
    async fn test_conn_recovery_drop_with_rollback() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );

        server
            .send(&vec![Query::new("BEGIN").into()].into())
            .await
            .unwrap();

        loop {
            let msg = server.read().await.unwrap();
            if msg.code() == 'Z' {
                break;
            }
        }

        assert!(server.in_transaction());

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::Drop)
            .await
            .unwrap();

        use crate::state::State;
        assert_eq!(server.stats().get_state(), State::ForceClose);
        assert!(server.in_transaction());
    }

    #[tokio::test]
    async fn test_conn_recovery_drop_with_needs_drain_and_rollback() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );
        server
            .prepared_statements_mut()
            .configure(PreparedStatementsConfig {
                limit: 1,
                ..Default::default()
            });

        server
            .send(
                &vec![
                    ProtocolMessage::from(Parse::named("test_0", "SELECT 1")),
                    Flush.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        let ok = server.read().await.unwrap();
        assert_eq!(ok.code(), '1');
        assert!(server.done());

        server
            .send(&vec![Query::new("BEGIN").into()].into())
            .await
            .unwrap();

        loop {
            let msg = server.read().await.unwrap();
            if msg.code() == 'Z' {
                break;
            }
        }

        assert!(server.in_transaction());

        server
            .send(&vec![Query::new("SHOW prepared_statements").into()].into())
            .await
            .unwrap();

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        assert!(server.needs_drain());
        assert!(server.in_transaction());

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::Drop)
            .await
            .unwrap();

        use crate::state::State;
        assert_eq!(server.stats().get_state(), State::ForceClose);
        assert!(server.needs_drain());
        assert!(server.in_transaction());
    }

    #[tokio::test]
    async fn test_cleanup_syncs_prepared_statements() {
        crate::logger();

        let mut server = Guard::new(
            Pool::new_test(),
            Box::new(test_server().await),
            Instant::now(),
        );

        assert!(!server.sync_prepared());

        server
            .send(&vec![Query::new("PREPARE test_stmt AS SELECT $1::bigint").into()].into())
            .await
            .unwrap();

        for c in ['C', 'Z'] {
            let msg = server.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        assert!(
            server.sync_prepared(),
            "sync_prepared flag should be set after PREPARE command"
        );

        let mut guard = server;
        let mut server = guard.server.take().unwrap();
        let cleanup = Cleanup::new(&guard, &mut server);

        Recovery::cleanup_internal(&mut server, cleanup, ConnectionRecovery::Recover)
            .await
            .unwrap();

        assert!(
            !server.sync_prepared(),
            "sync_prepared flag should be cleared after cleanup"
        );

        assert!(
            server.prepared_statements_mut().contains("test_stmt"),
            "Statement should be in local cache after sync"
        );

        let one: Vec<i32> = server.fetch_all("SELECT 1").await.unwrap();
        assert_eq!(one[0], 1);
    }

    #[tokio::test]
    async fn test_sending_request_false_initially() {
        crate::logger();

        let server = test_server().await;

        assert!(
            !server.is_sending_request(),
            "sending_request should be false initially"
        );
    }

    #[tokio::test]
    async fn test_sending_request_false_after_successful_send() {
        crate::logger();

        let mut server = test_server().await;

        server
            .send(&vec![Query::new("SELECT 1").into()].into())
            .await
            .unwrap();

        assert!(
            !server.is_sending_request(),
            "sending_request should be false after successful send"
        );
    }

    #[tokio::test]
    async fn test_interrupted_send_force_closes_on_checkin() {
        crate::logger();

        let pool = pool();

        {
            let mut guard = safe_timeout(Duration::from_secs(5), pool.get(&Request::default()))
                .await
                .expect("timed out getting connection")
                .unwrap();

            // Send a very large query that will timeout during send
            let large_query = (0..50_000_000).map(|_| 'b').collect::<String>();
            let large_query = Query::new(format!("SELECT '{}'", large_query));

            let res = safe_timeout(
                Duration::from_millis(1),
                guard.send(&vec![large_query.into()].into()),
            )
            .await;

            assert!(res.is_err(), "send should timeout");
            assert!(
                guard.is_sending_request(),
                "sending_request should be true after interrupted send"
            );
        }

        safe_sleep(Duration::from_millis(100)).await;

        let state = pool.state();
        assert_eq!(
            state.force_close, 1,
            "force_close should be incremented when connection has interrupted send"
        );
    }
}
