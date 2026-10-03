use std::{
    ops::{Deref, DerefMut},
    time::{Duration, SystemTime},
};

use tokio::select;
use tracing::{debug, error, trace};

use crate::{
    backend::{ConnectReason, Server},
    net::DataRow,
    tasks,
};

use super::lsn_cache::LsnCache;
use super::*;
use pgdog_postgres_types::Format;

use crate::util::{safe_interval, safe_sleep, safe_timeout};
use pgdog_stats::LsnStats as StatsLsnStats;
pub(crate) use pgdog_stats::replication::ReplicaLag;

static AURORA_DETECTION_QUERY: &str = "SELECT aurora_version()";

static LSN_QUERY: &str = "
SELECT
    pg_is_in_recovery() AS replica,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(
                pg_last_wal_replay_lsn(),
                pg_last_wal_receive_lsn()
            )
        ELSE
            pg_current_wal_lsn()
    END AS lsn,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(
                pg_last_wal_replay_lsn(),
                pg_last_wal_receive_lsn()
            ) - '0/0'::pg_lsn
        ELSE
            pg_current_wal_lsn() - '0/0'::pg_lsn
    END AS offset_bytes,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(pg_last_xact_replay_timestamp(), now())
        ELSE
            now()
    END AS timestamp
";

static AURORA_LSN_QUERY: &str = "
SELECT
    pg_is_in_recovery() AS replica,
    '0/0'::pg_lsn AS lsn,
    0::bigint AS offset_bytes,
    now() AS timestamp
";

/// LSN information.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LsnStats {
    inner: StatsLsnStats,
}

impl Deref for LsnStats {
    type Target = StatsLsnStats;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for LsnStats {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<StatsLsnStats> for LsnStats {
    fn from(value: StatsLsnStats) -> Self {
        Self { inner: value }
    }
}

impl LsnStats {
    /// How old the stats are.
    pub(crate) fn lsn_age(&self, now: SystemTime) -> Duration {
        now.duration_since(self.fetched).unwrap_or_default()
    }

    /// Stats contain real data.
    pub(crate) fn valid(&self) -> bool {
        self.inner.valid()
    }
}

impl LsnStats {
    fn from_row(value: DataRow, aurora: bool) -> Self {
        StatsLsnStats {
            replica: value.get(0, Format::Text).unwrap_or_default(),
            lsn: value.get(1, Format::Text).unwrap_or_default(),
            offset_bytes: value.get(2, Format::Text).unwrap_or_default(),
            timestamp: value.get(3, Format::Text).unwrap_or_default(),
            fetched: SystemTime::now(),
            aurora,
        }
        .into()
    }
}

/// LSN monitor loop.
pub(super) struct LsnMonitor {
    pool: Pool,
}

impl LsnMonitor {
    pub(super) fn run(pool: &Pool) {
        let monitor = Self { pool: pool.clone() };

        tasks::spawn("pool lsn monitor", async move {
            monitor.spawn().await;
        });
    }

    async fn run_query(&self, conn: &mut Server, query: &str) -> Option<DataRow> {
        match safe_timeout(self.pool.config().lsn_check_timeout, conn.fetch_all(query)).await {
            Ok(Ok(rows)) => rows.into_iter().next(),
            Ok(Err(err)) => {
                error!("lsn monitor query error: {} [{}]", err, self.pool.addr());
                None
            }
            Err(_) => {
                error!("lsn monitor query timeout [{}]", self.pool.addr());
                None
            }
        }
    }

    async fn detect_aurora(&self, conn: &mut Server) -> Option<bool> {
        match safe_timeout(
            self.pool.config().lsn_check_timeout,
            conn.fetch_all::<DataRow>(AURORA_DETECTION_QUERY),
        )
        .await
        {
            Ok(Ok(_)) => {
                debug!("aurora detected [{}]", self.pool.addr());
                Some(true)
            }
            Ok(Err(crate::backend::Error::ExecutionError(_))) => Some(false),
            Ok(Err(err)) => {
                error!(
                    "lsn monitor aurora detection error: {} [{}]",
                    err,
                    self.pool.addr()
                );
                None
            }
            Err(_) => {
                error!(
                    "lsn monitor aurora detection timeout [{}]",
                    self.pool.addr()
                );
                None
            }
        }
    }

    async fn spawn(&self) {
        select! {
            _ = safe_sleep(self.pool.config().lsn_check_delay) => {},
            _ = self.pool.comms().shutdown.cancelled() => { return; }
        }

        debug!("lsn monitor loop is running [{}]", self.pool.addr());

        let mut aurora_detected: Option<bool> = None;
        let mut interval = safe_interval(self.pool.config().lsn_check_interval);

        loop {
            select! {
                _ = interval.tick() => {},
                _ = self.pool.comms().shutdown.cancelled() => { break; }
            }

            match self.check(aurora_detected).await {
                Ok(result) => aurora_detected = result,
                Err(Error::Offline) => break,
                Err(_) => continue,
            }
        }

        debug!("lsn monitor shutdown [{}]", self.pool.addr());
    }

    /// Run the LSN check, unless another pool connected to the same
    /// server just did, in which case its stats apply to this pool too.
    async fn check(&self, aurora_detected: Option<bool>) -> Result<Option<bool>, Error> {
        let cache = LsnCache::global();
        let addr = self.pool.addr();
        let max_age = self.pool.config().lsn_check_interval / 2;

        // Wait for another pool checking the server, but not for longer
        // than it takes for its stats to go stale.
        let _lock = safe_timeout(max_age, cache.lock(addr)).await;

        if let Some(stats) = cache.recent(addr, max_age) {
            self.update_stats(stats);
            trace!("lsn monitor stats reused [{}]", addr);
            return Ok(aurora_detected);
        }

        self.run_check(aurora_detected).await
    }

    async fn run_check(&self, mut aurora_detected: Option<bool>) -> Result<Option<bool>, Error> {
        let mut conn = match self.get_connection().await {
            Ok(conn) => conn,
            Err(Error::Offline) => return Err(Error::Offline),
            Err(err) => {
                error!("lsn monitor checkout error: {} [{}]", err, self.pool.addr());
                return Err(err);
            }
        };

        if aurora_detected.is_none() {
            aurora_detected = self.detect_aurora(&mut conn).await;
        }

        // Aurora detection failed, try again next iteration.
        let Some(aurora) = aurora_detected else {
            return Ok(None);
        };

        let query = if aurora { AURORA_LSN_QUERY } else { LSN_QUERY };

        if let Some(row) = self.run_query(&mut conn, query).await {
            drop(conn);
            let stats = LsnStats::from_row(row, aurora);
            LsnCache::global().set(self.pool.addr(), stats);
            self.update_stats(stats);
            trace!("lsn monitor stats updated [{}]", self.pool.addr());
        }

        Ok(aurora_detected)
    }

    fn update_stats(&self, stats: LsnStats) {
        let mut guard = self.pool.inner().lsn_stats.write();
        // Notify that the role changed and the shard monitor
        // should immediately resynchronize.
        if stats.replica != guard.replica {
            self.pool.inner().lsn_role_change.notify_one();
        }
        (*guard) = stats;
    }

    async fn get_connection(&self) -> Result<LsnConnection, Error> {
        match self.pool.get(&Request::default()).await {
            Ok(conn) => Ok(LsnConnection::Guard(conn)),
            Err(Error::Offline) => Err(Error::Offline),
            Err(Error::CheckoutTimeout) => Ok(LsnConnection::Conn(Box::new(
                self.pool.standalone(ConnectReason::LsnCheck).await?,
            ))),
            Err(err) => Err(err),
        }
    }
}

enum LsnConnection {
    Guard(Guard),
    Conn(Box<Server>),
}

impl Deref for LsnConnection {
    type Target = Server;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Guard(guard) => guard.deref(),
            Self::Conn(server) => server,
        }
    }
}

impl DerefMut for LsnConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Guard(guard) => guard.deref_mut(),
            Self::Conn(server) => server,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashSet;

    use futures::future::join_all;
    use pgdog_postgres_types::TimestampTz;
    use pgdog_stats::Lsn;
    use tokio::time::{sleep, timeout};

    // A launched pool against the local Postgres. The default `lsn_check_delay`
    // is `MAX_DURATION`, so the background LSN monitor spawned by `launch()`
    // stays asleep and never competes with the `run_check` calls below.
    fn monitor() -> LsnMonitor {
        crate::logger();
        let pool = Pool::new_test();
        pool.launch();
        LsnMonitor { pool }
    }

    #[tokio::test]
    async fn test_run_check_detects_non_aurora() {
        let monitor = monitor();

        // No prior detection: run_check must detect Aurora (false locally),
        // run the standard LSN query and update the stats.
        let result = monitor.run_check(None).await;
        assert_eq!(result, Ok(Some(false)));

        let stats = monitor.pool.lsn_stats();
        assert!(stats.valid(), "stats should be valid after a check");
        assert!(!stats.replica, "local Postgres is a primary");
        assert!(!stats.aurora, "local Postgres is not Aurora");
        assert!(stats.lsn.lsn > 0, "primary LSN should advance past 0");
        assert!(stats.offset_bytes > 0, "offset bytes should be positive");

        monitor.pool.shutdown();
    }

    #[tokio::test]
    async fn test_run_check_skips_aurora_detection_when_known() {
        let monitor = monitor();

        // Detection already done: run_check must reuse `Some(false)` and still
        // produce valid stats via the standard query.
        let result = monitor.run_check(Some(false)).await;
        assert_eq!(result, Ok(Some(false)));

        let stats = monitor.pool.lsn_stats();
        assert!(stats.valid());
        assert!(!stats.aurora);
        assert!(stats.lsn.lsn > 0);

        monitor.pool.shutdown();
    }

    #[tokio::test]
    async fn test_run_check_aurora_query_path() {
        let monitor = monitor();

        // When told the server is Aurora, run_check uses the Aurora query,
        // which reports a zero LSN. Aurora stats are still valid at LSN 0.
        let result = monitor.run_check(Some(true)).await;
        assert_eq!(result, Ok(Some(true)));

        let stats = monitor.pool.lsn_stats();
        assert!(stats.aurora, "stats should be flagged Aurora");
        assert!(stats.valid(), "Aurora stats are valid even at LSN 0");
        assert_eq!(stats.lsn.lsn, 0, "Aurora query reports zero LSN");
        assert_eq!(stats.offset_bytes, 0);
        assert!(!stats.replica);

        monitor.pool.shutdown();
    }

    #[tokio::test]
    async fn test_run_check_offline_returns_offline() {
        let monitor = monitor();
        monitor.pool.shutdown();

        // A shut-down pool can't hand out connections: checkout returns
        // Offline and run_check propagates it so the loop can break.
        let result = monitor.run_check(None).await;
        assert_eq!(result, Err(Error::Offline));
    }

    #[tokio::test]
    async fn test_run_check_notifies_on_role_change() {
        let monitor = monitor();

        // Seed the stats as if the server were previously seen as a replica.
        *monitor.pool.inner().lsn_stats.write() = StatsLsnStats {
            replica: true,
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: false,
        }
        .into();

        // The check observes the local primary (replica = false), a role
        // change, so it must fire the role-change notification.
        let result = monitor.run_check(Some(false)).await;
        assert_eq!(result, Ok(Some(false)));
        assert!(!monitor.pool.lsn_stats().replica);

        assert!(
            timeout(
                Duration::from_millis(200),
                monitor.pool.inner().lsn_role_change.notified()
            )
            .await
            .is_ok(),
            "role change should have been notified"
        );

        monitor.pool.shutdown();
    }

    #[tokio::test]
    async fn test_run_check_no_notify_without_role_change() {
        let monitor = monitor();

        // Establish the current role first. The seed default is "replica",
        // so this first check flips to primary and fires one notification —
        // drain that stored permit before testing the steady state.
        assert_eq!(monitor.run_check(Some(false)).await, Ok(Some(false)));
        let _ = safe_timeout(
            Duration::from_millis(50),
            monitor.pool.inner().lsn_role_change.notified(),
        )
        .await;

        // A second check sees the same role, so no notification fires and
        // the `notified()` future stays pending until the timeout.
        assert_eq!(monitor.run_check(Some(false)).await, Ok(Some(false)));

        assert!(
            timeout(
                Duration::from_millis(100),
                monitor.pool.inner().lsn_role_change.notified()
            )
            .await
            .is_err(),
            "no role change should mean no notification"
        );

        monitor.pool.shutdown();
    }

    #[tokio::test]
    async fn test_get_connection_falls_back_to_standalone_when_saturated() {
        crate::logger();

        // Keep the timeout bounded while allowing the initial connection to be
        // established on slower test hosts.
        let config = Config {
            max: 1,
            min: 1,
            checkout_timeout: Duration::from_secs(1),
            ..Config::default()
        };

        let pool = Pool::new(&PoolConfig {
            address: Address::new_test(),
            config,
        });
        pool.launch();

        // Saturate the pool by holding its only connection.
        let _hold = pool.get(&Request::default()).await.unwrap();
        assert_eq!(pool.lock().idle(), 0);

        // A normal checkout now hits the checkout timeout.
        assert_eq!(
            pool.get(&Request::default()).await.unwrap_err(),
            Error::CheckoutTimeout
        );

        // Checkout times out, so get_connection opens a standalone connection
        // instead of stalling the LSN loop.
        let monitor = LsnMonitor { pool: pool.clone() };
        let mut conn = monitor.get_connection().await.unwrap();
        assert!(
            matches!(conn, LsnConnection::Conn(_)),
            "saturated pool should yield a standalone connection"
        );

        // The standalone connection is usable.
        assert!(monitor.run_query(&mut conn, LSN_QUERY).await.is_some());

        pool.shutdown();
    }

    #[test]
    fn test_aurora_stats_valid_with_zero_lsn() {
        let stats: LsnStats = StatsLsnStats {
            replica: true,
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: true,
        }
        .into();

        assert!(
            stats.valid(),
            "Aurora stats should be valid even with zero LSN"
        );
    }

    #[test]
    fn test_non_aurora_stats_invalid_with_zero_lsn() {
        let stats: LsnStats = StatsLsnStats {
            replica: true,
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: false,
        }
        .into();

        assert!(
            !stats.valid(),
            "Non-Aurora stats should be invalid with zero LSN"
        );
    }

    fn lsn_stats(replica: bool, lsn: i64) -> LsnStats {
        StatsLsnStats {
            replica,
            lsn: Lsn::from_i64(lsn),
            offset_bytes: lsn,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: false,
        }
        .into()
    }

    // A pool for one of the test databases on the local Postgres.
    fn server_pool(database: &str, lsn_check_interval: Duration) -> Pool {
        Pool::new(&PoolConfig {
            address: Address {
                database_name: database.into(),
                ..Address::new_test()
            },
            config: Config {
                lsn_check_interval,
                ..Config::default()
            },
        })
    }

    #[tokio::test]
    async fn test_check_reuses_stats_from_same_server() {
        crate::logger();

        let pgdog = server_pool("pgdog", Duration::from_secs(5));
        pgdog.launch();
        // Never launched, so it can't check out a connection. It only gets
        // stats by reusing the ones fetched for the other database.
        let shard_0 = server_pool("shard_0", Duration::from_secs(5));

        LsnMonitor {
            pool: pgdog.clone(),
        }
        .check(None)
        .await
        .unwrap();

        let result = LsnMonitor {
            pool: shard_0.clone(),
        }
        .check(None)
        .await;
        assert_eq!(result, Ok(None), "no aurora detection, no query");

        let (fetched, reused) = (pgdog.lsn_stats(), shard_0.lsn_stats());
        assert!(reused.valid());
        assert!(!reused.replica);
        assert_eq!(reused.lsn, fetched.lsn);
        assert_eq!(reused.fetched, fetched.fetched);

        pgdog.shutdown();
    }

    #[tokio::test]
    async fn test_concurrent_checks_query_server_once() {
        crate::logger();

        let pools: Vec<_> = ["pgdog", "shard_0", "shard_1", "shard_2"]
            .into_iter()
            .map(|database| server_pool(database, Duration::from_secs(5)))
            .collect();
        let monitors: Vec<_> = pools
            .iter()
            .map(|pool| {
                pool.launch();
                LsnMonitor { pool: pool.clone() }
            })
            .collect();

        // Every pool checks at the same time, like when their intervals line up.
        let results = join_all(monitors.iter().map(|monitor| monitor.check(None))).await;
        assert!(results.iter().all(Result::is_ok), "{:?}", results);

        // One pool queried the server, the others reused its stats.
        let fetched: HashSet<_> = pools.iter().map(|pool| pool.lsn_stats().fetched).collect();
        assert_eq!(fetched.len(), 1);
        assert!(pools.iter().all(|pool| pool.lsn_stats().valid()));

        for pool in pools {
            pool.shutdown();
        }
    }

    #[tokio::test]
    async fn test_check_queries_server_when_shared_stats_are_stale() {
        crate::logger();

        let pool = server_pool("pgdog", Duration::from_millis(100));
        pool.launch();
        let monitor = LsnMonitor { pool: pool.clone() };

        // Another pool checked the server more than half an interval ago.
        let stale = lsn_stats(true, 1);
        LsnCache::global().set(pool.addr(), stale);
        sleep(Duration::from_millis(100)).await;

        assert_eq!(monitor.check(None).await, Ok(Some(false)));

        let stats = pool.lsn_stats();
        assert!(!stats.replica);
        assert!(stats.lsn.lsn > 1);

        // The new stats replace the stale ones for other pools.
        let shared = LsnCache::global()
            .recent(pool.addr(), Duration::from_secs(5))
            .expect("fresh stats are shared");
        assert_eq!(shared.fetched, stats.fetched);

        pool.shutdown();
    }

    #[tokio::test]
    async fn test_check_does_not_wait_for_slow_check() {
        crate::logger();

        let pool = server_pool("pgdog", Duration::from_millis(100));
        pool.launch();
        let monitor = LsnMonitor { pool: pool.clone() };

        // Another pool is stuck checking the same server.
        let _slow = LsnCache::global().lock(pool.addr()).await;

        // After half an interval, the pool stops waiting and checks the server itself.
        let result = timeout(Duration::from_secs(5), monitor.check(None)).await;
        assert_eq!(result, Ok(Ok(Some(false))));
        assert!(pool.lsn_stats().valid());

        pool.shutdown();
    }

    #[tokio::test]
    async fn test_check_notifies_on_role_change_from_shared_stats() {
        crate::logger();

        // Nothing listens on this port, so the pool can only reuse stats.
        let pool = Pool::new(&PoolConfig {
            address: Address {
                port: 1,
                ..Address::new_test()
            },
            config: Config::default(),
        });
        assert!(pool.lsn_stats().replica);
        let monitor = LsnMonitor { pool: pool.clone() };

        // Another pool found out the server is a primary.
        LsnCache::global().set(pool.addr(), lsn_stats(false, 100));

        assert_eq!(monitor.check(None).await, Ok(None));
        assert!(!pool.lsn_stats().replica);
        assert!(
            timeout(
                Duration::from_millis(200),
                pool.inner().lsn_role_change.notified()
            )
            .await
            .is_ok(),
            "role change should have been notified"
        );
    }
}
