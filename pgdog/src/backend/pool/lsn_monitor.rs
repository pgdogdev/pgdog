use tokio::{
    spawn,
    time::{sleep, timeout},
};
use tracing::{debug, error, trace};

use crate::{
    backend::replication::publisher::Lsn,
    net::{DataRow, Format, TimestampTz},
};

use super::*;

// Fields are as follows:
//
// - 1. true if replica, false otherwise
// - 2. bytes offset in WAL
// - 4. timestamp of last transaction if replica, now if primary
//
static QUERY: &'static str = "
SELECT
    pg_is_in_recovery() AS replica,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(
                pg_last_wal_receive_lsn(),
                pg_last_wal_replay_lsn()
            )
        ELSE
            pg_current_wal_lsn()
    END AS lsn,
    CASE
        WHEN pg_is_in_recovery() THEN
            COALESCE(
                pg_last_wal_receive_lsn(),
                pg_last_wal_replay_lsn()
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

#[derive(Debug, Clone, Copy, Default)]
pub struct LsnStats {
    pub replica: bool,
    pub lsn: Lsn,
    pub offset_bytes: i64,
    pub timestamp: TimestampTz,
}

impl From<DataRow> for LsnStats {
    fn from(value: DataRow) -> Self {
        Self {
            replica: value.get(0, Format::Text).unwrap_or_default(),
            lsn: value.get(1, Format::Text).unwrap_or_default(),
            offset_bytes: value.get(2, Format::Text).unwrap_or_default(),
            timestamp: value.get(3, Format::Text).unwrap_or_default(),
        }
    }
}

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
        debug!("lsn monitor loop is running [{}]", self.pool.addr());

        loop {
            sleep(self.pool.config().lsn_check_interval).await;

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

            if let Some(stats) = stats.pop() {
                {
                    self.pool.lock().lsn_stats = stats;
                }
                trace!("lsn monitor stats updated [{}]", self.pool.addr());
            }
        }

        debug!("lsn monitor shutdown [{}]", self.pool.addr());
    }
}
