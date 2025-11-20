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

static QUERY: &'static str = "
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

/// LSN information.
#[derive(Debug, Clone, Copy)]
pub struct LsnStats {
    /// pg_is_in_recovery()
    pub replica: bool,
    /// Replay LSN oon replica, current LSN on primary
    pub lsn: Lsn,
    /// LSN position in bytes from 0
    pub offset_bytes: i64,
    /// Server timestamp.
    pub timestamp: TimestampTz,
    /// Our timestamp.
    pub fetched: Instant,
}

impl Default for LsnStats {
    fn default() -> Self {
        Self {
            replica: true, // Replica unless proven otherwise.
            lsn: Lsn::default(),
            offset_bytes: 0,
            timestamp: TimestampTz::default(),
            fetched: Instant::now(),
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
        self.lsn.lsn > 0
    }
}
impl From<DataRow> for LsnStats {
    fn from(value: DataRow) -> Self {
        Self {
            replica: value.get(0, Format::Text).unwrap_or_default(),
            lsn: value.get(1, Format::Text).unwrap_or_default(),
            offset_bytes: value.get(2, Format::Text).unwrap_or_default(),
            timestamp: value.get(3, Format::Text).unwrap_or_default(),
            fetched: Instant::now(),
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

    async fn spawn(&self) {
        select! {
            _ = sleep(self.pool.config().lsn_check_delay) => {},
            _ = self.pool.comms().shutdown.notified() => { return; }
        }

        debug!("lsn monitor loop is running [{}]", self.pool.addr());

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

            let mut stats = match timeout(
                self.pool.config().lsn_check_timeout,
                conn.fetch_all::<LsnStats>(QUERY),
            )
            .await
            {
                Ok(Ok(stats)) => stats,

                Ok(Err(err)) => {
                    error!("lsn monitor query error: {} [{}]", err, self.pool.addr());
                    continue;
                }

                Err(_) => {
                    error!("lsn monitor query timeout [{}]", self.pool.addr());
                    continue;
                }
            };
            drop(conn);

            if let Some(stats) = stats.pop() {
                {
                    *self.pool.inner().lsn_stats.write() = stats;
                }
                trace!("lsn monitor stats updated [{}]", self.pool.addr());
            }
        }

        debug!("lsn monitor shutdown [{}]", self.pool.addr());
    }
}
