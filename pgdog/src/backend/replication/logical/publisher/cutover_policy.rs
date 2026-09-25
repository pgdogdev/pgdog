use std::time::Duration;

use pgdog_config::{ConfigAndUsers, CutoverTimeoutAction};
use tokio::{select, time::Instant};
use tracing::{info, warn};

use super::super::Error;
use super::Lsn;
use super::replication_progress::ReplicationProgress;
use crate::util::{format_bytes, human_duration, safe_interval, safe_timeout};
use pgdog_stats::ReplicationCutoverReason as CutoverReason;

const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug)]
pub(crate) struct CutoverConfig {
    /// Check when replication_lag becomes less than this value
    /// to stop the traffic on source.
    pub(crate) traffic_stop_threshold: u64,
    /// Start the cutover if replication_lag is less than this value
    pub(crate) replication_lag_threshold: u64,
    /// Start the cutover if last_transaction was more than this value time ago
    pub(crate) last_transaction_delay: Duration,
    /// Start/abort the wait for cutover after timeout
    pub(crate) timeout: Duration,
    pub(crate) timeout_action: CutoverTimeoutAction,
}

impl From<&ConfigAndUsers> for CutoverConfig {
    fn from(config: &ConfigAndUsers) -> Self {
        let general = &config.config.general;
        Self {
            traffic_stop_threshold: general.cutover_traffic_stop_threshold,
            replication_lag_threshold: general.cutover_replication_lag_threshold,
            last_transaction_delay: Duration::from_millis(general.cutover_last_transaction_delay),
            timeout: Duration::from_millis(general.cutover_timeout),
            timeout_action: general.cutover_timeout_action,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CutoverPolicy {
    config: CutoverConfig,
    progress: ReplicationProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub(crate) enum CutoverAction {
    Go(CutoverReason),
    NoGo(CutoverData),
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub(crate) struct CutoverData {
    pub(crate) lag: u64,
    pub(crate) last_transaction: Option<Duration>,
    pub(crate) elapsed: Duration,
}

impl CutoverPolicy {
    pub(crate) fn new(config: CutoverConfig, progress: ReplicationProgress) -> Self {
        Self { config, progress }
    }

    /// Resolves when replication_lag reaches the value less than
    /// configured [CutoverConfig::traffic_stop_threshold].
    /// After this source should stop any write activity and
    /// [`CutoverPolicy::wait_for_catchup`] should be started.
    pub(crate) async fn wait_for_stop_threshold(&self) {
        let traffic_stop = self.config.traffic_stop_threshold;

        info!(
            "[cutover] started, waiting for traffic stop threshold={}",
            format_bytes(traffic_stop)
        );

        let mut check = safe_interval(Duration::from_secs(1));

        loop {
            check.tick().await;

            let Some(lag) = self.progress.snapshot().lag_bytes else {
                info!("[cutover] replication lag is not calculated for all shards, yet");
                continue;
            };

            info!("[cutover] replication lag: {}", format_bytes(lag));

            if lag <= traffic_stop {
                info!(
                    "[cutover] stopping traffic, lag={}, threshold={}",
                    format_bytes(lag),
                    format_bytes(traffic_stop),
                );
                break;
            }
        }
    }

    fn should_cutover(&self, start: Instant) -> CutoverAction {
        let cutover_timeout = self.config.timeout;
        let cutover_threshold = self.config.replication_lag_threshold;
        let last_transaction_delay = self.config.last_transaction_delay;

        // Read elapsed before the snapshot and compare strictly: both values are
        // floored to milliseconds, so a measurement older than `start` never passes.
        let elapsed = start.elapsed();
        let progress = self.progress.snapshot();
        let lag = progress.lag_bytes.filter(|_| {
            progress
                .lag_age_ms
                .is_some_and(|age| u128::from(age) < elapsed.as_millis())
        });
        let last_transaction = progress.last_transaction_ms.map(Duration::from_millis);
        let cutover_timeout_exceeded = elapsed >= cutover_timeout;

        if cutover_timeout_exceeded {
            CutoverAction::Go(CutoverReason::Timeout)
        } else if lag.is_some_and(|lag| lag <= cutover_threshold) {
            CutoverAction::Go(CutoverReason::Lag)
        } else if last_transaction.is_some_and(|t| t > last_transaction_delay) {
            CutoverAction::Go(CutoverReason::LastTransaction)
        } else {
            CutoverAction::NoGo(CutoverData {
                lag: lag.unwrap_or(u64::MAX),
                last_transaction,
                elapsed,
            })
        }
    }

    /// Wait until cutover conditions are met depending on the
    /// [`CutoverConfig`] settings, then until every shard applied
    /// the source WAL written before this call.
    pub(crate) async fn wait_for_catchup(&self) -> Result<CutoverReason, Error> {
        let cutover_timeout_action = self.config.timeout_action;

        info!(
            "[cutover] waiting for first cutover threshold: timeout={}, transaction={}, lag={}",
            human_duration(self.config.timeout),
            human_duration(self.config.last_transaction_delay),
            format_bytes(self.config.replication_lag_threshold)
        );

        let mut check = safe_interval(Duration::from_millis(50));
        let mut log = safe_interval(Duration::from_secs(1));
        let start = Instant::now();

        let mut cutover_data = None;

        let reason = loop {
            select! {
                _ = check.tick() => {}
                _ = log.tick() => {
                    if let Some(CutoverData { lag, last_transaction, elapsed }) = cutover_data {
                        info!(
                            "[cutover] lag={}, last_transaction={}, timeout={}",
                            format_bytes(lag),
                            if let Some(last_transaction) = last_transaction {
                                human_duration(last_transaction)
                            } else {
                                "none".into()
                            },
                            human_duration(elapsed),
                        );
                    }
                }
            }

            match self.should_cutover(start) {
                CutoverAction::Go(CutoverReason::Timeout) => match cutover_timeout_action {
                    CutoverTimeoutAction::Abort => {
                        warn!("[cutover] abort timeout reached, resuming traffic");
                        return Err(Error::AbortTimeout);
                    }
                    CutoverTimeoutAction::Cutover => break CutoverReason::Timeout,
                },
                CutoverAction::Go(reason) => break reason,
                CutoverAction::NoGo(data) => cutover_data = Some(data),
            }
        };

        info!(
            "[cutover] {reason} reached, waiting until every shard applies the source WAL written before the traffic stop"
        );
        self.wait_for_source_wal(start).await?;
        info!("[cutover] performing cutover now, reason: {reason}");
        Ok(reason)
    }

    /// Wait until we catch up to the last seen source LSN at the moment
    /// we trigger the cutover. This to replicate all the data
    /// that could be committed before cutover
    async fn wait_for_source_wal(&self, since: Instant) -> Result<(), Error> {
        let mut check = safe_interval(Duration::from_millis(50));
        safe_timeout(CATCH_UP_TIMEOUT, async {
            let targets = loop {
                check.tick().await;
                if let Some(targets) = self.target_lsns(since) {
                    break targets;
                }
            };
            while !self.applied_target_lsns(&targets) {
                check.tick().await;
            }
        })
        .await
        .map_err(|_| Error::CatchUpTimeout)
    }

    fn target_lsns(&self, since: Instant) -> Option<Vec<Lsn>> {
        let mut targets = Vec::with_capacity(self.progress.len());
        for shard in 0..self.progress.len() {
            let progress = self.progress.shard(shard)?;
            if progress.source_measured_at.is_none_or(|at| at < since) {
                return None;
            }
            targets.push(progress.source_lsn?);
        }
        Some(targets)
    }

    fn applied_target_lsns(&self, targets: &[Lsn]) -> bool {
        for (shard, target) in targets.iter().enumerate() {
            let applied = self
                .progress
                .shard(shard)
                .and_then(|progress| progress.applied_lsn);
            if applied.is_none_or(|applied| applied < *target) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::replication::logical::publisher::Lsn;
    use crate::backend::replication::logical::publisher::replication_progress::ReplicationProgress;
    use crate::util::safe_timeout;
    use std::assert_matches;
    use tokio::time::Instant;

    fn cutover_config() -> CutoverConfig {
        CutoverConfig {
            traffic_stop_threshold: 1000,
            replication_lag_threshold: 100,
            last_transaction_delay: Duration::from_millis(500),
            timeout: Duration::from_secs(10),
            timeout_action: CutoverTimeoutAction::Abort,
        }
    }

    fn measure(progress: &ReplicationProgress, shard: usize, source: i64, applied: i64) {
        progress.updater_for_shard(shard).update(|s| {
            s.replication_lag = Some(source - applied);
            s.source_measured_at = Some(Instant::now());
            s.source_lsn = Some(Lsn::from_i64(source));
            s.applied_lsn = Some(Lsn::from_i64(applied));
        });
    }

    #[tokio::test]
    async fn test_wait_for_replication_exits_when_lag_below_threshold() {
        let config = cutover_config();

        let progress = ReplicationProgress::new(2);
        progress
            .updater_for_shard(0)
            .update(|s| s.replication_lag = Some(500));
        progress
            .updater_for_shard(1)
            .update(|s| s.replication_lag = Some(500));

        let waiter = CutoverPolicy::new(config, progress);

        safe_timeout(Duration::from_secs(5), waiter.wait_for_stop_threshold())
            .await
            .expect("the wait must exit once every shard is below the threshold");
    }

    #[tokio::test(start_paused = true)]
    async fn test_should_cutover_ignores_lag_measured_before_start() {
        let config = cutover_config();
        let start = Instant::now();
        tokio::time::advance(Duration::from_millis(10)).await;

        let progress = ReplicationProgress::new(2);
        for shard in 0..2 {
            progress.updater_for_shard(shard).update(|s| {
                s.replication_lag = Some(50);
                s.source_measured_at = Some(Instant::now());
            });
        }
        tokio::time::advance(Duration::from_millis(10)).await;

        let waiter = CutoverPolicy::new(config, progress);

        assert_eq!(
            waiter.should_cutover(start),
            CutoverAction::Go(CutoverReason::Lag)
        );
        assert_matches!(
            waiter.should_cutover(Instant::now()),
            CutoverAction::NoGo { .. }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_wait_for_cutover_waits_for_lag_measured_after_start() {
        let config = cutover_config();
        let stale = Instant::now();

        let progress = ReplicationProgress::new(2);
        for shard in 0..2 {
            progress.updater_for_shard(shard).update(|s| {
                s.replication_lag = Some(50);
                s.source_measured_at = Some(stale);
            });
        }
        tokio::time::advance(Duration::from_millis(10)).await;

        let waiter = CutoverPolicy::new(config, progress.clone());
        let refresh = Duration::from_millis(300);

        let ((reason, returned_at), ()) = tokio::join!(
            async { (waiter.wait_for_catchup().await, Instant::now()) },
            async {
                tokio::time::sleep(refresh).await;
                for shard in 0..2 {
                    measure(&progress, shard, 100, 100);
                }
            }
        );

        assert_eq!(reason.unwrap(), CutoverReason::Lag);
        assert!(returned_at - stale >= refresh);
    }

    #[tokio::test(start_paused = true)]
    async fn test_wait_for_cutover_exits_when_last_transaction_old() {
        let config = CutoverConfig {
            replication_lag_threshold: 10,
            last_transaction_delay: Duration::from_millis(100),
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(200));
        });

        let waiter = CutoverPolicy::new(config, progress.clone());

        assert_eq!(
            waiter.should_cutover(Instant::now()),
            CutoverAction::Go(CutoverReason::LastTransaction)
        );

        let (reason, ()) = tokio::join!(waiter.wait_for_catchup(), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            measure(&progress, 0, 1000, 1000);
        });
        assert_eq!(reason.unwrap(), CutoverReason::LastTransaction);
    }

    #[tokio::test]
    async fn test_cutover_timeout_aborts_when_configured() {
        let config = CutoverConfig {
            timeout: Duration::ZERO,
            timeout_action: CutoverTimeoutAction::Abort,
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(1);
        progress
            .updater_for_shard(0)
            .update(|s| s.replication_lag = Some(5000));

        let waiter = CutoverPolicy::new(config, progress);

        assert_eq!(
            waiter.should_cutover(Instant::now()),
            CutoverAction::Go(CutoverReason::Timeout)
        );
        assert_matches!(waiter.wait_for_catchup().await, Err(Error::AbortTimeout));
    }

    #[tokio::test(start_paused = true)]
    async fn test_cutover_timeout_cuts_over_when_configured() {
        let config = CutoverConfig {
            timeout: Duration::ZERO,
            timeout_action: CutoverTimeoutAction::Cutover,
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(1);
        progress
            .updater_for_shard(0)
            .update(|s| s.replication_lag = Some(5000));

        let waiter = CutoverPolicy::new(config, progress.clone());

        let (reason, ()) = tokio::join!(waiter.wait_for_catchup(), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            measure(&progress, 0, 5000, 5000);
        });
        assert_eq!(reason.unwrap(), CutoverReason::Timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn test_wait_for_catchup_waits_for_source_wal_measured_after_start() {
        let config = CutoverConfig {
            replication_lag_threshold: 10,
            last_transaction_delay: Duration::from_millis(100),
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(1);
        measure(&progress, 0, 100, 100);
        progress.updater_for_shard(0).update(|s| {
            s.last_transaction = Some(Instant::now() - Duration::from_millis(200));
        });
        tokio::time::advance(Duration::from_millis(10)).await;

        let waiter = CutoverPolicy::new(config, progress.clone());
        let start = Instant::now();
        let step = Duration::from_millis(300);

        let ((reason, returned_at), ()) = tokio::join!(
            async { (waiter.wait_for_catchup().await, Instant::now()) },
            async {
                tokio::time::sleep(step).await;
                measure(&progress, 0, 200, 100);
                tokio::time::sleep(step).await;
                progress
                    .updater_for_shard(0)
                    .update(|s| s.applied_lsn = Some(Lsn::from_i64(200)));
            }
        );

        assert_eq!(reason.unwrap(), CutoverReason::LastTransaction);
        assert!(returned_at - start >= step * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_wait_for_catchup_fails_when_source_wal_is_not_applied() {
        let config = CutoverConfig {
            timeout: Duration::ZERO,
            timeout_action: CutoverTimeoutAction::Cutover,
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(1);
        let waiter = CutoverPolicy::new(config, progress.clone());

        let (result, ()) = tokio::join!(waiter.wait_for_catchup(), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            measure(&progress, 0, 200, 100);
        });
        assert_matches!(result, Err(Error::CatchUpTimeout));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_no_transaction_was_applied() {
        let config = CutoverConfig {
            replication_lag_threshold: 10,
            last_transaction_delay: Duration::from_millis(100),
            ..cutover_config()
        };

        let since = Instant::now() - Duration::from_millis(100);
        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.source_measured_at = Some(Instant::now());
        });

        let waiter = CutoverPolicy::new(config, progress);

        assert!(matches!(
            waiter.should_cutover(since),
            CutoverAction::NoGo(_)
        ));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_above_threshold_and_recent_transaction() {
        let config = cutover_config();

        let since = Instant::now() - Duration::from_millis(100);
        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.source_measured_at = Some(Instant::now());
            s.last_transaction = Some(Instant::now() - Duration::from_millis(50));
        });

        let waiter = CutoverPolicy::new(config, progress);

        assert!(matches!(
            waiter.should_cutover(since),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn test_should_not_cutover_when_timeout_not_reached() {
        let config = CutoverConfig {
            timeout: Duration::from_secs(1),
            replication_lag_threshold: 10,
            ..cutover_config()
        };

        let since = Instant::now() - Duration::from_millis(999);
        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.source_measured_at = Some(Instant::now());
            s.last_transaction = Some(Instant::now() - Duration::from_millis(100));
        });

        let waiter = CutoverPolicy::new(config, progress);

        assert!(matches!(
            waiter.should_cutover(since),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_just_above_threshold() {
        let config = cutover_config();

        let since = Instant::now() - Duration::from_millis(100);
        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(101);
            s.source_measured_at = Some(Instant::now());
            s.last_transaction = Some(Instant::now() - Duration::from_millis(50));
        });

        let waiter = CutoverPolicy::new(config, progress);

        assert!(matches!(
            waiter.should_cutover(since),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn should_not_cutover_before_every_shard_reports() {
        let config = CutoverConfig {
            replication_lag_threshold: 1000,
            ..cutover_config()
        };

        let since = Instant::now() - Duration::from_millis(100);
        let progress = ReplicationProgress::new(2);
        progress
            .updater_for_shard(0)
            .update(|s| s.last_transaction = Some(Instant::now()));

        let waiter = CutoverPolicy::new(config, progress.clone());

        assert_eq!(progress.snapshot().lag_bytes, None);
        assert_matches!(waiter.should_cutover(since), CutoverAction::NoGo { .. });

        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(500);
            s.source_measured_at = Some(Instant::now());
        });
        assert_eq!(progress.snapshot().lag_bytes, None);
        assert_matches!(waiter.should_cutover(since), CutoverAction::NoGo { .. });

        progress.updater_for_shard(1).update(|s| {
            s.replication_lag = Some(400);
            s.source_measured_at = Some(Instant::now());
        });
        assert_eq!(
            waiter.should_cutover(since),
            CutoverAction::Go(CutoverReason::Lag)
        );
    }
}
