use std::sync::Arc;
use std::time::Duration;

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

    /// Calculate the combined replication lag for all the shards stream.
    /// None is returned if some of the progress was not yet updated
    pub(crate) fn replication_lag(&self) -> Option<u64> {
        let mut max: Option<i64> = None;
        for shard in self.shards.iter() {
            let lag = shard.lock().replication_lag?;
            max = Some(max.map_or(lag, |m| m.max(lag)));
        }
        max.map(|l| l.max(0) as u64)
    }

    /// Get the time elapsed from most recent transaction update for a progress
    pub(crate) fn last_transaction(&self) -> Option<Duration> {
        self.shards
            .iter()
            .filter_map(|shard| shard.lock().last_transaction)
            .max()
            .map(|t| t.elapsed())
    }

    /// Get the pgdog_stats representation for progress
    pub(crate) fn snapshot(&self) -> pgdog_stats::ReplicationProgress {
        pgdog_stats::ReplicationProgress {
            lag_bytes: self.replication_lag(),
            last_transaction_ms: self
                .last_transaction()
                .map(|elapsed| elapsed.as_millis() as u64),
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

        assert_eq!(progress.replication_lag(), None);

        progress
            .updater_for_shard(0)
            .update(|p| p.replication_lag = Some(100));
        assert_eq!(progress.replication_lag(), None);

        progress
            .updater_for_shard(1)
            .update(|p| p.replication_lag = Some(200));
        assert_eq!(progress.replication_lag(), None);

        progress
            .updater_for_shard(2)
            .update(|p| p.replication_lag = Some(150));
        assert_eq!(progress.replication_lag(), Some(200));
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

        assert_eq!(progress.last_transaction(), None);

        let older = tokio::time::Instant::now() - Duration::from_millis(300);
        progress
            .updater_for_shard(0)
            .update(|p| p.last_transaction = Some(older));

        tokio::time::advance(Duration::from_millis(10)).await;
        let recent = tokio::time::Instant::now();
        progress
            .updater_for_shard(1)
            .update(|p| p.last_transaction = Some(recent));

        let elapsed = progress
            .last_transaction()
            .expect("at least one shard has a transaction");
        assert_eq!(elapsed, Duration::ZERO);
    }
}
