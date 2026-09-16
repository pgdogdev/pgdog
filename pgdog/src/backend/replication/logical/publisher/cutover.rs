use std::{fmt::Display, sync::Arc, time::Duration};

use pgdog_config::{ConfigAndUsers, CutoverTimeoutAction};
use tokio::{select, time::Instant};
use tracing::{info, warn};

use super::super::Error;
use super::replication_progress::ReplicationProgress;
use crate::util::{format_bytes, human_duration, safe_interval};

#[derive(Debug)]
pub(crate) struct Cutover {
    config: Arc<ConfigAndUsers>,
    progress: ReplicationProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub(crate) enum CutoverReason {
    Lag,
    Timeout,
    LastTransaction,
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

impl Display for CutoverReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lag => write!(f, "lag"),
            Self::Timeout => write!(f, "timeout"),
            Self::LastTransaction => write!(f, "last_transaction"),
        }
    }
}

impl Cutover {
    pub(crate) fn new(config: Arc<ConfigAndUsers>, progress: ReplicationProgress) -> Self {
        Self { config, progress }
    }

    pub(crate) async fn wait_for_replication(&self) -> Result<(), Error> {
        let traffic_stop = self.config.config.general.cutover_traffic_stop_threshold;

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
        let cutover_timeout = Duration::from_millis(self.config.config.general.cutover_timeout);
        let cutover_threshold = self.config.config.general.cutover_replication_lag_threshold;
        let last_transaction_delay =
            Duration::from_millis(self.config.config.general.cutover_last_transaction_delay);

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

    pub(crate) async fn wait_for_cutover(&self) -> Result<(), Error> {
        let cutover_threshold = self.config.config.general.cutover_replication_lag_threshold;
        let last_transaction_delay =
            Duration::from_millis(self.config.config.general.cutover_last_transaction_delay);
        let cutover_timeout = Duration::from_millis(self.config.config.general.cutover_timeout);
        let cutover_timeout_action = self.config.config.general.cutover_timeout_action;

        info!(
            "[cutover] waiting for first cutover threshold: timeout={}, transaction={}, lag={}",
            human_duration(cutover_timeout),
            human_duration(last_transaction_delay),
            format_bytes(cutover_threshold)
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
                CutoverAction::Go(CutoverReason::Timeout) => {
                    if cutover_timeout_action == CutoverTimeoutAction::Abort {
                        warn!("[cutover] abort timeout reached, resuming traffic");
                        return Err(Error::AbortTimeout);
                    } else {
                        info!(
                            "[cutover] performing cutover now, reason: {}",
                            CutoverReason::Timeout
                        );
                        break;
                    }
                }
                CutoverAction::NoGo(data) => {
                    cutover_data = Some(data);
                    continue;
                }
                CutoverAction::Go(reason) => {
                    info!("[cutover] performing cutover now, reason: {}", reason);
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::replication::logical::publisher::replication_progress::ReplicationProgress;
    use crate::util::{safe_sleep, safe_timeout};
    use pgdog_config::ConfigAndUsers;
    use std::assert_matches;
    use std::sync::Arc;
    use tokio::time::Instant;

    #[tokio::test]
    async fn test_wait_for_replication_exits_when_lag_below_threshold() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_traffic_stop_threshold = 1000;

        let progress = ReplicationProgress::new(2);
        progress.shard(0).update(|s| s.replication_lag = Some(500));
        progress.shard(1).update(|s| s.replication_lag = Some(500));

        let waiter = Cutover::new(Arc::new(config), progress);
        let result = waiter.wait_for_replication().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_cutover_exits_when_lag_below_threshold() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_replication_lag_threshold = 100;
        config.config.general.cutover_timeout = 10000;

        let progress = ReplicationProgress::new(2);
        progress.shard(0).update(|s| s.replication_lag = Some(50));
        progress.shard(1).update(|s| s.replication_lag = Some(50));

        let waiter = Cutover::new(Arc::new(config), progress);

        assert_eq!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::Go(CutoverReason::Lag)
        );

        let result = waiter.wait_for_cutover().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_cutover_exits_when_last_transaction_old() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_replication_lag_threshold = 10;
        config.config.general.cutover_last_transaction_delay = 100;
        config.config.general.cutover_timeout = 10000;

        let progress = ReplicationProgress::new(1);
        progress.shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(200));
        });

        let waiter = Cutover::new(Arc::new(config), progress);

        assert_eq!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::Go(CutoverReason::LastTransaction)
        );

        let result = waiter.wait_for_cutover().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_should_cutover_when_no_transaction() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_replication_lag_threshold = 10;
        config.config.general.cutover_last_transaction_delay = 100;
        config.config.general.cutover_timeout = 10000;

        let progress = ReplicationProgress::new(1);
        progress.shard(0).update(|s| s.replication_lag = Some(1000));

        let waiter = Cutover::new(Arc::new(config), progress);

        assert_eq!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::Go(CutoverReason::LastTransaction)
        );
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_above_threshold_and_recent_transaction() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 10000;
        config.config.general.cutover_replication_lag_threshold = 100;
        config.config.general.cutover_last_transaction_delay = 500;

        let progress = ReplicationProgress::new(1);
        progress.shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(50));
        });

        let waiter = Cutover::new(Arc::new(config), progress);

        assert!(matches!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_timeout_not_reached() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 1000;
        config.config.general.cutover_replication_lag_threshold = 10;
        config.config.general.cutover_last_transaction_delay = 500;

        let progress = ReplicationProgress::new(1);
        progress.shard(0).update(|s| {
            s.replication_lag = Some(1000);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(100));
        });

        let waiter = Cutover::new(Arc::new(config), progress);

        assert!(matches!(
            waiter.should_cutover(Duration::from_millis(999)),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_just_above_threshold() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 10000;
        config.config.general.cutover_replication_lag_threshold = 100;
        config.config.general.cutover_last_transaction_delay = 500;

        let progress = ReplicationProgress::new(1);
        progress.shard(0).update(|s| {
            s.replication_lag = Some(101);
            s.last_transaction = Some(Instant::now() - Duration::from_millis(50));
        });

        let waiter = Cutover::new(Arc::new(config), progress);

        assert!(matches!(
            waiter.should_cutover(Duration::from_millis(100)),
            CutoverAction::NoGo { .. }
        ));
    }

    #[tokio::test]
    async fn should_not_cutover_before_every_shard_reports() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 10000;
        config.config.general.cutover_replication_lag_threshold = 1000;
        config.config.general.cutover_last_transaction_delay = 500;

        let progress = ReplicationProgress::new(2);
        progress
            .shard(0)
            .update(|s| s.last_transaction = Some(Instant::now()));

        let waiter = Cutover::new(Arc::new(config), progress.clone());
        let elapsed = Duration::from_millis(100);

        assert_eq!(progress.replication_lag(), None);
        assert_matches!(waiter.should_cutover(elapsed), CutoverAction::NoGo { .. });

        progress.shard(0).update(|s| s.replication_lag = Some(500));
        assert_eq!(progress.replication_lag(), None);
        assert_matches!(waiter.should_cutover(elapsed), CutoverAction::NoGo { .. });

        progress.shard(1).update(|s| s.replication_lag = Some(400));
        assert_eq!(
            waiter.should_cutover(elapsed),
            CutoverAction::Go(CutoverReason::Lag)
        );
    }

    #[tokio::test]
    async fn wait_for_replication_finishes_with_unrelated_writes() {
        use crate::backend::replication::logical::publisher::publisher_impl::Publisher;
        use crate::backend::server::test::test_server;

        crate::logger();

        const TRAFFIC_STOP: u64 = 1_000;

        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_traffic_stop_threshold = TRAFFIC_STOP;
        config.config.general.cutover_timeout = 120_000;

        let cluster = crate::backend::pool::Cluster::new_test(&config);
        let publication = "test_pub".to_owned();
        let slot = "test_slot".to_owned();
        let shards = cluster.shards().len();

        let mut source = test_server().await;
        let _ = source
            .execute(format!("DROP PUBLICATION IF EXISTS {publication}"))
            .await;
        for shard in 0..shards {
            let _ = source
                .execute(format!("SELECT pg_drop_replication_slot('{slot}_{shard}')"))
                .await;
        }
        source
            .execute("DROP TABLE IF EXISTS issue1_main, issue1_noise")
            .await
            .unwrap();
        source
            .execute("CREATE TABLE issue1_main (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        source
            .execute("CREATE TABLE issue1_noise (id BIGINT, payload TEXT)")
            .await
            .unwrap();
        source
            .execute(format!(
                "CREATE PUBLICATION {publication} FOR TABLE issue1_main"
            ))
            .await
            .unwrap();

        cluster.launch();

        let stop = tokio_util::sync::CancellationToken::new();
        let mut publisher = Publisher::new(&publication, slot.clone());
        let streams = publisher
            .prepare_replication(&cluster, &stop)
            .await
            .unwrap();
        let progress = ReplicationProgress::new(shards);
        let tasks: Vec<_> = streams
            .into_iter()
            .map(|stream| {
                let updater = progress.shard(stream.source_shard);
                let task = crate::api::replication::ReplicationSlotTask::new(
                    stream,
                    &cluster,
                    &cluster,
                    stop.clone(),
                    updater,
                );
                crate::api::run_task(task)
            })
            .collect();

        let config = Arc::new(config);
        let waiter = Cutover::new(config, progress);

        source
            .execute(
                "INSERT INTO issue1_noise \
                 SELECT g, (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 64)) \
                 FROM generate_series(1, 5000) g",
            )
            .await
            .unwrap();

        safe_sleep(Duration::from_secs(1)).await;

        let result = safe_timeout(Duration::from_secs(20), waiter.wait_for_replication()).await;

        stop.cancel();
        let mut drained = Ok(());
        for task in tasks {
            drained = drained.and(task.await);
        }
        for shard in 0..shards {
            let _ = source
                .execute(format!("SELECT pg_drop_replication_slot('{slot}_{shard}')"))
                .await;
        }
        let _ = source
            .execute(format!("DROP PUBLICATION IF EXISTS {publication}"))
            .await;
        let _ = source
            .execute("DROP TABLE IF EXISTS issue1_main, issue1_noise")
            .await;

        drained.expect("replication tasks failed while stopping");
        let waited = result
            .expect("wait_for_replication never finished: lag stays inflated by unrelated WAL");
        waited.expect("wait_for_replication returned an error");
    }
}
