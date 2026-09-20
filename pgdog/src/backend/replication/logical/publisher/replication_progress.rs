use std::sync::Arc;

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::backend::replication::publisher::Lsn;
use pgdog_stats::MissedRows;

/// Tracks the progress of replication for
/// a single source shard
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReplicationShardProgress {
    pub(crate) replication_lag: Option<i64>,
    pub(crate) last_transaction: Option<Instant>,
    pub(crate) applied_lsn: Option<Lsn>,
    pub(crate) missed_rows: MissedRows,
    pub(crate) bytes_sharded: usize,
    pub(crate) origin_lsn: Lsn,
}

impl ReplicationShardProgress {
    pub(crate) fn advance_applied_lsn(&mut self, applied: Lsn) {
        self.applied_lsn = Some(
            self.applied_lsn
                .map_or(applied, |current| current.max(applied)),
        );
    }

    pub(crate) fn snapshot(&self, fallback_lsn: Lsn) -> pgdog_stats::ReplicationShardStatus {
        pgdog_stats::ReplicationShardStatus {
            lsn: self.applied_lsn.unwrap_or(fallback_lsn),
            lag_bytes: self.replication_lag,
            missed_rows: self.missed_rows,
        }
    }
}

/// Tracks the progress for all of the source shards
#[derive(Clone, Debug)]
pub(crate) struct ReplicationProgress {
    shards: Arc<[Mutex<ReplicationShardProgress>]>,
}

impl ReplicationProgress {
    pub(crate) fn new(shard_count: usize) -> Self {
        let shards = (0..shard_count).map(|_| Mutex::default()).collect();
        Self { shards }
    }

    /// Returns the entity to update the progress for a single source shard
    pub(crate) fn updater_for_shard(&self, shard: usize) -> ReplicationProgressShardUpdater {
        assert!(
            shard < self.shards.len(),
            "shard {shard} is out of range for {} shards",
            self.shards.len()
        );

        ReplicationProgressShardUpdater {
            shards: self.shards.clone(),
            shard,
        }
    }

    /// The combined progress of every shard, as reported to `SHOW TASKS` and
    /// read by the cutover policy. `lag_bytes` stays `None` until every shard
    /// has reported one.
    pub(crate) fn snapshot(&self) -> pgdog_stats::ReplicationProgress {
        let mut lag: Option<i64> = None;
        let mut every_shard_reported = true;
        let mut last_transaction: Option<Instant> = None;

        for shard in self.shards.iter() {
            let shard = *shard.lock();
            match shard.replication_lag {
                Some(shard_lag) => lag = Some(lag.map_or(shard_lag, |max| max.max(shard_lag))),
                None => every_shard_reported = false,
            }
            if let Some(applied) = shard.last_transaction {
                last_transaction = Some(last_transaction.map_or(applied, |max| max.max(applied)));
            }
        }

        pgdog_stats::ReplicationProgress {
            lag_bytes: every_shard_reported
                .then_some(lag)
                .flatten()
                .map(|lag| lag.max(0) as u64),
            last_transaction_ms: last_transaction
                .map(|applied| applied.elapsed().as_millis() as u64),
        }
    }
}

/// Used to update the progress of a single source shard stream.
#[derive(Clone, Debug)]
pub(crate) struct ReplicationProgressShardUpdater {
    shards: Arc<[Mutex<ReplicationShardProgress>]>,
    shard: usize,
}

impl ReplicationProgressShardUpdater {
    pub(crate) fn update(&self, f: impl FnOnce(&mut ReplicationShardProgress)) {
        f(&mut self.shards[self.shard].lock());
    }

    pub(crate) fn snapshot(&self) -> ReplicationShardProgress {
        *self.shards[self.shard].lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn lag_none_until_all_shards_report() {
        let progress = ReplicationProgress::new(3);

        assert_eq!(progress.snapshot().lag_bytes, None);

        progress
            .updater_for_shard(0)
            .update(|p| p.replication_lag = Some(100));
        assert_eq!(progress.snapshot().lag_bytes, None);

        progress
            .updater_for_shard(1)
            .update(|p| p.replication_lag = Some(200));
        assert_eq!(progress.snapshot().lag_bytes, None);

        progress
            .updater_for_shard(2)
            .update(|p| p.replication_lag = Some(150));
        assert_eq!(progress.snapshot().lag_bytes, Some(200));
    }

    #[test]
    fn cloned_updater_shares_shard_state() {
        let progress = ReplicationProgress::new(2);
        let a = progress.updater_for_shard(0);
        let b = a.clone();

        a.update(|p| p.replication_lag = Some(77));
        assert_eq!(b.snapshot().replication_lag, Some(77));

        b.update(|p| p.replication_lag = Some(99));
        assert_eq!(a.snapshot().replication_lag, Some(99));
    }

    #[test]
    fn updaters_for_different_shards_are_independent() {
        let progress = ReplicationProgress::new(2);
        progress
            .updater_for_shard(0)
            .update(|p| p.replication_lag = Some(10));
        progress
            .updater_for_shard(1)
            .update(|p| p.replication_lag = Some(20));

        assert_eq!(
            progress.updater_for_shard(0).snapshot().replication_lag,
            Some(10)
        );
        assert_eq!(
            progress.updater_for_shard(1).snapshot().replication_lag,
            Some(20)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn last_transaction_returns_most_recent_across_shards() {
        let progress = ReplicationProgress::new(2);

        assert_eq!(progress.snapshot().last_transaction_ms, None);

        let older = tokio::time::Instant::now() - Duration::from_millis(300);
        progress
            .updater_for_shard(0)
            .update(|p| p.last_transaction = Some(older));

        tokio::time::advance(Duration::from_millis(10)).await;
        let recent = tokio::time::Instant::now();
        progress
            .updater_for_shard(1)
            .update(|p| p.last_transaction = Some(recent));

        assert_eq!(progress.snapshot().last_transaction_ms, Some(0));
    }
}
