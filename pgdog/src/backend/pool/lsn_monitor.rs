use std::time::Duration;

use tokio::{
    select, spawn,
    time::{interval, sleep, timeout, Instant},
};
use tracing::{debug, error, trace};

use crate::{
    backend::replication::publisher::Lsn,
    net::{DataRow, Format, TimestampTz},
};

use super::*;

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
#[derive(Debug, Clone, Copy)]
pub struct LsnStats {
    /// pg_is_in_recovery()
    pub replica: bool,
    /// Replay LSN on replica, current LSN on primary.
    pub lsn: Lsn,
    /// LSN position in bytes from 0.
    pub offset_bytes: i64,
    /// Server timestamp.
    pub timestamp: TimestampTz,
    /// Our timestamp.
    pub fetched: Instant,
    /// Running on Aurora.
    pub aurora: bool,
}

impl Default for LsnStats {
    fn default() -> Self {
        Self {
            replica: true, // Replica unless proven otherwise.
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: Instant::now(),
            aurora: false,
        }
    }
}

impl LsnStats {
    /// How old the stats are.
    pub fn lsn_age(&self, now: Instant) -> Duration {
        now.duration_since(self.fetched)
    }

    /// Stats contain real data.
    pub fn valid(&self) -> bool {
        self.aurora || self.lsn.lsn > 0
    }
}
impl LsnStats {
    fn from_row(value: DataRow, aurora: bool) -> Self {
        Self {
            replica: value.get(0, Format::Text).unwrap_or_default(),
            lsn: value.get(1, Format::Text).unwrap_or_default(),
            offset_bytes: value.get(2, Format::Text).unwrap_or_default(),
            timestamp: value.get(3, Format::Text).unwrap_or_default(),
            fetched: Instant::now(),
            aurora,
        }
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

    #[test]
    fn test_aurora_stats_valid_with_zero_lsn() {
        let stats = LsnStats {
            replica: true,
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: Instant::now(),
            aurora: true,
        };

        assert!(
            stats.valid(),
            "Aurora stats should be valid even with zero LSN"
        );
    }

    #[test]
    fn test_non_aurora_stats_invalid_with_zero_lsn() {
        let stats = LsnStats {
            replica: true,
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: Instant::now(),
            aurora: false,
        };

        assert!(
            !stats.valid(),
            "Non-Aurora stats should be invalid with zero LSN"
        );
    }
}
