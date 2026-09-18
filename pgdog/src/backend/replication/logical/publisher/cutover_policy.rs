use std::time::Duration;

use pgdog_config::{ConfigAndUsers, CutoverTimeoutAction};
use tokio::{select, time::Instant};
use tracing::{info, warn};

use super::super::Error;
use super::replication_progress::ReplicationProgress;
use crate::util::{format_bytes, human_duration, safe_interval};
use pgdog_stats::ReplicationCutoverReason as CutoverReason;

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
    pub(crate) async fn wait_for_stop_threshold(&self) -> Result<(), Error> {
        let traffic_stop = self.config.traffic_stop_threshold;

        info!(
            "[cutover] started, waiting for traffic stop threshold={}",
            format_bytes(traffic_stop)
        );

        let mut check = safe_interval(Duration::from_secs(1));

        loop {
            check.tick().await;

            let Some(lag) = self.progress.replication_lag() else {
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

        Ok(())
    }

    fn should_cutover(&self, elapsed: Duration) -> CutoverAction {
        let cutover_timeout = self.config.timeout;
        let cutover_threshold = self.config.replication_lag_threshold;
        let last_transaction_delay = self.config.last_transaction_delay;

        let lag = self.progress.replication_lag();
        let last_transaction = self.progress.last_transaction();
        let cutover_timeout_exceeded = elapsed >= cutover_timeout;

        if cutover_timeout_exceeded {
            CutoverAction::Go(CutoverReason::Timeout)
        } else if lag.is_some_and(|lag| lag <= cutover_threshold) {
            CutoverAction::Go(CutoverReason::Lag)
        } else if last_transaction.is_none_or(|t| t > last_transaction_delay) {
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
    /// [`CutoverConfig`] settings
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

        loop {
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

            let elapsed = start.elapsed();

            match self.should_cutover(elapsed) {
                CutoverAction::Go(CutoverReason::Timeout) => match cutover_timeout_action {
                    CutoverTimeoutAction::Abort => {
                        warn!("[cutover] abort timeout reached, resuming traffic");
                        return Err(Error::AbortTimeout);
                    }
                    CutoverTimeoutAction::Cutover => {
                        info!(
                            "[cutover] performing cutover now, reason: {}",
                            CutoverReason::Timeout
                        );
                        return Ok(CutoverReason::Timeout);
                    }
                },
                CutoverAction::Go(reason) => {
                    info!("[cutover] performing cutover now, reason: {reason}");
                    return Ok(reason);
                }
                CutoverAction::NoGo(data) => {
                    cutover_data = Some(data);
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::replication::logical::publisher::replication_progress::ReplicationProgress;
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
        let result = waiter.wait_for_stop_threshold().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_cutover_exits_when_lag_below_threshold() {
        let config = cutover_config();

        let progress = ReplicationProgress::new(2);
        progress
            .updater_for_shard(0)
            .update(|s| s.replication_lag = Some(50));
        progress
            .updater_for_shard(1)
            .update(|s| s.replication_lag = Some(50));

        let waiter = CutoverPolicy::new(config, progress);

        assert_eq!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::Go(CutoverReason::Lag)
        );

        assert_eq!(waiter.wait_for_catchup().await.unwrap(), CutoverReason::Lag);
    }

    #[tokio::test]
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

        let waiter = CutoverPolicy::new(config, progress);

        assert_eq!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::Go(CutoverReason::LastTransaction)
        );

        assert_eq!(
            waiter.wait_for_catchup().await.unwrap(),
            CutoverReason::LastTransaction
        );
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
            waiter.should_cutover(Duration::ZERO),
            CutoverAction::Go(CutoverReason::Timeout)
        );
        assert_matches!(waiter.wait_for_catchup().await, Err(Error::AbortTimeout));
    }

    #[tokio::test]
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

        let waiter = CutoverPolicy::new(config, progress);

        assert_eq!(
            waiter.wait_for_catchup().await.unwrap(),
            CutoverReason::Timeout
        );
    }

    #[tokio::test]
    async fn test_should_cutover_when_no_transaction() {
        let config = CutoverConfig {
            replication_lag_threshold: 10,
            last_transaction_delay: Duration::from_millis(100),
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(1);
        progress
            .updater_for_shard(0)
            .update(|s| s.replication_lag = Some(1000));

        let waiter = CutoverPolicy::new(config, progress);

        assert_eq!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::Go(CutoverReason::LastTransaction)
        );
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_above_threshold_and_recent_transaction() {
        let config = cutover_config();

        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(50));
        });

        let waiter = CutoverPolicy::new(config, progress);

        assert!(matches!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_timeout_not_reached() {
        let config = CutoverConfig {
            timeout: Duration::from_secs(1),
            replication_lag_threshold: 10,
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(100));
        });

        let waiter = CutoverPolicy::new(config, progress);

        assert!(matches!(
            waiter.should_cutover(Duration::from_millis(999)),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_just_above_threshold() {
        let config = cutover_config();

        let progress = ReplicationProgress::new(1);
        progress.updater_for_shard(0).update(|s| {
            s.replication_lag = Some(101);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(50));
        });

        let waiter = CutoverPolicy::new(config, progress);

        assert!(matches!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn should_not_cutover_before_every_shard_reports() {
        let config = CutoverConfig {
            replication_lag_threshold: 1000,
            ..cutover_config()
        };

        let progress = ReplicationProgress::new(2);
        progress
            .updater_for_shard(0)
            .update(|s| s.last_transaction = Some(Instant::now()));

        let waiter = CutoverPolicy::new(config, progress.clone());
        let elapsed = Duration::from_millis(100);

        assert_eq!(progress.replication_lag(), None);
        assert_matches!(waiter.should_cutover(elapsed), CutoverAction::NoGo { .. });

        progress
            .updater_for_shard(0)
            .update(|s| s.replication_lag = Some(500));
        assert_eq!(progress.replication_lag(), None);
        assert_matches!(waiter.should_cutover(elapsed), CutoverAction::NoGo { .. });

        progress
            .updater_for_shard(1)
            .update(|s| s.replication_lag = Some(400));
        assert_eq!(
            waiter.should_cutover(elapsed),
            CutoverAction::Go(CutoverReason::Lag)
        );
    }
}
