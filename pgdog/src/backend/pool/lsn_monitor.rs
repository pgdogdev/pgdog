use std::{
    ops::{Deref, DerefMut},
    time::{Duration, SystemTime},
};

use tokio::{
    select, spawn,
    time::{interval, sleep, timeout},
};
use tracing::{debug, error, trace};

use crate::net::DataRow;

use super::*;
use pgdog_postgres_types::Format;

use pgdog_stats::LsnStats as StatsLsnStats;

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
pub struct LsnStats {
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
    pub fn lsn_age(&self, now: SystemTime) -> Duration {
        now.duration_since(self.fetched).unwrap_or_default()
    }

    /// Stats contain real data.
    pub fn valid(&self) -> bool {
        self.aurora || self.lsn.lsn > 0
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

        spawn(async move {
            monitor.spawn().await;
        });
    }

    async fn run_query(&self, conn: &mut Guard, query: &str) -> Option<DataRow> {
        match timeout(self.pool.config().lsn_check_timeout, conn.fetch_all(query)).await {
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

    async fn detect_aurora(&self, conn: &mut Guard) -> Option<bool> {
        match timeout(
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
            _ = sleep(self.pool.config().lsn_check_delay) => {},
            _ = self.pool.comms().shutdown.notified() => { return; }
        }

        debug!("lsn monitor loop is running [{}]", self.pool.addr());

        let mut aurora_detected: Option<bool> = None;
        let mut interval = interval(self.pool.config().lsn_check_interval);

        loop {
            select! {
                _ = interval.tick() => {},
                _ = self.pool.comms().shutdown.notified() => { break; }
            }

            let mut conn = match self.pool.get(&Request::default()).await {
                Ok(conn) => conn,
                Err(Error::Offline) => break,
                Err(err) => {
                    error!("lsn monitor checkout error: {} [{}]", err, self.pool.addr());
                    continue;
                }
            };

            if aurora_detected.is_none() {
                aurora_detected = self.detect_aurora(&mut conn).await;
            }

            let Some(aurora) = aurora_detected else {
                continue;
            };

            let query = if aurora { AURORA_LSN_QUERY } else { LSN_QUERY };

            if let Some(row) = self.run_query(&mut conn, query).await {
                drop(conn);
                let stats = LsnStats::from_row(row, aurora);
                *self.pool.inner().lsn_stats.write() = stats;
                trace!("lsn monitor stats updated [{}]", self.pool.addr());
            }
        }

        debug!("lsn monitor shutdown [{}]", self.pool.addr());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use pgdog_postgres_types::TimestampTz;
    use pgdog_stats::Lsn;

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
}
