use crate::{
    backend::{Cluster, databases::cancel_all, maintenance_mode},
    tasks,
    util::{format_bytes, human_duration, random_string},
};
use pgdog_config::{ConfigAndUsers, CutoverTimeoutAction};
use pgdog_stats::Databases;
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{Mutex, MutexGuard},
    time::Instant,
};
use tracing::{error, info, warn};

use super::*;
use crate::util::safe_interval;

#[derive(Debug, Clone)]
pub(crate) struct Orchestrator {
    pub(crate) source: Cluster,
    pub(crate) destination: Cluster,
    pub(crate) publication: String,
    publisher: Arc<Mutex<Publisher>>,
    replication_slot: String,
}

/// A handle to a publication's replication slots, decoupled from the rest of
/// the orchestrator. Awaiting [`PublicationGuard::cleanup`] drops every slot
/// the publisher still owns — a no-op once `replicate` has handed them off to
/// the streaming tasks. Dropping an armed guard schedules cleanup when its
/// owning task is aborted; successful migrations must disarm it.
pub(crate) struct PublicationGuard {
    publisher: Option<Arc<Mutex<Publisher>>>,
}

impl PublicationGuard {
    /// Drop any replication slots the publisher still owns.
    pub(crate) async fn cleanup(mut self) -> Result<(), Error> {
        let publisher = self.publisher.as_ref().expect("publication guard is armed");
        let result = Box::pin(publisher.lock().await.cleanup()).await;
        self.publisher.take();
        result
    }

    /// Preserve the slots when a migration completes successfully.
    pub(crate) fn disarm(mut self) {
        self.publisher.take();
    }
}

impl Drop for PublicationGuard {
    fn drop(&mut self) {
        let Some(publisher) = self.publisher.take() else {
            return;
        };
        tasks::spawn("replication slot cleanup", async move {
            if let Err(err) = Box::pin(publisher.lock().await.cleanup()).await {
                warn!("failed to clean up replication slots after an aborted migration: {err}");
            }
        });
    }
}

impl Orchestrator {
    /// Create new orchestrator.
    pub(crate) fn new(
        source: &str,
        destination: &str,
        publication: &str,
        replication_slot: Option<String>,
    ) -> Result<Self, Error> {
        let source = databases().schema_owner(source)?;
        let destination = databases().schema_owner(destination)?;

        let replication_slot = replication_slot
            .unwrap_or(format!("__pgdog_repl_{}", random_string(19).to_lowercase()));

        let mut orchestrator = Self {
            source,
            destination,
            publication: publication.to_owned(),
            publisher: Arc::new(Mutex::new(Publisher::default())),
            replication_slot,
        };

        orchestrator.refresh_publisher();

        Ok(orchestrator)
    }

    /// Reload source/dest cluster references from the live databases registry.
    pub(crate) fn refresh(&mut self) -> Result<(), Error> {
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination = databases().schema_owner(&self.destination.identifier().database)?;
        Ok(())
    }

    /// Replace the publisher entirely (discards LSN state).  Only valid
    /// when starting a fresh replication phase, e.g. after cutover.
    pub(crate) fn refresh_publisher(&mut self) {
        let publisher = Publisher::new(&self.publication, self.replication_slot.clone());
        self.publisher = Arc::new(Mutex::new(publisher));
    }

    pub(crate) fn replication_slot(&self) -> &str {
        &self.replication_slot
    }

    pub(crate) async fn publisher(&self) -> MutexGuard<'_, Publisher> {
        self.publisher.lock().await
    }

    /// Take a [`PublicationGuard`] over this orchestrator's replication slots.
    pub(crate) fn publication_guard(&self) -> PublicationGuard {
        PublicationGuard {
            publisher: Some(self.publisher.clone()),
        }
    }

    /// Get the largest replication lag out of all the shards.
    async fn replication_lag(&self) -> Option<u64> {
        let shards_count = self.source.shards().len();
        let lag = self.publisher.lock().await.replication_lag();

        if lag.len() != shards_count {
            // if the len of lag map is not equal to source shards_count
            // then some entries are not initialized yet and the lag value
            // is not yet reported.
            return None;
        }

        lag.values().copied().max().map(|lag| lag as u64)
    }

    /// The two ends of the migration this orchestrator drives.
    pub(crate) fn databases(&self) -> Databases {
        Databases {
            source: self.source.identifier().database.clone(),
            destination: self.destination.identifier().database.clone(),
        }
    }
}

impl Display for Orchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {}",
            self.source.identifier().database,
            self.destination.identifier().database
        )
    }
}

#[derive(Debug, Display)]
#[display("{orchestrator}")]
pub(crate) struct Cutover {
    orchestrator: Orchestrator,
    config: Arc<ConfigAndUsers>,
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
    pub(crate) fn new(orchestrator: Orchestrator) -> Self {
        Self {
            orchestrator,
            config: config(),
        }
    }

    /// Wait for replication to catch up.
    pub(crate) async fn wait_for_replication(&mut self) -> Result<(), Error> {
        let traffic_stop = self.config.config.general.cutover_traffic_stop_threshold;

        info!(
            "[cutover] started, waiting for traffic stop threshold={}",
            format_bytes(traffic_stop)
        );

        // Check once a second how far we got.
        let mut check = safe_interval(Duration::from_secs(1));

        loop {
            check.tick().await;

            let Some(lag) = self.orchestrator.replication_lag().await else {
                info!("[cutover] replication lag is not calculated for all shards, yet");
                continue;
            };

            info!("[cutover] replication lag: {}", format_bytes(lag));

            // Time to go.
            if lag <= traffic_stop {
                info!(
                    "[cutover] stopping traffic, lag={}, threshold={}",
                    format_bytes(lag),
                    format_bytes(traffic_stop),
                );

                // Pause traffic.
                maintenance_mode::start(None);

                // Cancel any running queries.
                ok_or_abort!(cancel_all(&self.orchestrator.source.identifier().database).await);

                break;
                // TODO: wait for clients to all stop.
            }
        }

        Ok(())
    }

    async fn should_cutover(&self, elapsed: Duration) -> CutoverAction {
        let cutover_timeout = Duration::from_millis(self.config.config.general.cutover_timeout);
        let cutover_threshold = self.config.config.general.cutover_replication_lag_threshold;
        let last_transaction_delay =
            Duration::from_millis(self.config.config.general.cutover_last_transaction_delay);

        let lag = self.orchestrator.replication_lag().await;
        let last_transaction = self.orchestrator.publisher.lock().await.last_transaction();
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

    /// Wait for cutover.
    pub(crate) async fn wait_for_cutover(&mut self) -> Result<(), Error> {
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

        // Check more frequently.
        let mut check = safe_interval(Duration::from_millis(50));
        let mut log = safe_interval(Duration::from_secs(1));
        // Abort clock starts now.
        let start = Instant::now();

        let mut cutover_data = None;

        loop {
            select! {
                _ = check.tick() => {}

                _ = log.tick() => {
                    if let Some(CutoverData { lag, last_transaction, elapsed }) = cutover_data {
                        info!("[cutover] lag={}, last_transaction={}, timeout={}",
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
            let cutover_reason = self.should_cutover(elapsed).await;

            match cutover_reason {
                CutoverAction::Go(CutoverReason::Timeout) => {
                    if cutover_timeout_action == CutoverTimeoutAction::Abort {
                        maintenance_mode::stop(None);
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

macro_rules! ok_or_abort {
    ($expr:expr_2021) => {
        match $expr {
            Ok(res) => res,
            Err(err) => {
                error!("Orchestrator failed: {err}");
                maintenance_mode::stop(None);
                return Err(Error::from(err));
            }
        }
    };
}

use ok_or_abort;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::Cluster;
    use crate::util::{safe_sleep, safe_timeout};
    use pgdog_config::ConfigAndUsers;
    use std::assert_matches;
    use std::sync::Arc;
    use tokio::time::Instant;

    impl Orchestrator {
        fn new_test(config: &ConfigAndUsers) -> Self {
            let cluster = Cluster::new_test(config);
            let publication = "test_pub".to_owned();
            let replication_slot = "test_slot".to_owned();
            let publisher = Publisher::new(&publication, replication_slot.clone());
            Self {
                source: cluster.clone(),
                destination: cluster,
                publication,
                publisher: Arc::new(Mutex::new(publisher)),
                replication_slot,
            }
        }
    }

    impl Cutover {
        fn new_test(orchestrator: Orchestrator, config: Arc<ConfigAndUsers>) -> Self {
            Self {
                orchestrator,
                config,
            }
        }
    }

    #[tokio::test]
    async fn test_wait_for_replication_exits_when_lag_below_threshold() {
        // Ensure maintenance mode is off at start
        maintenance_mode::stop(None);
        assert!(!maintenance_mode::is_on("")); // Will return true because all databases are paused.

        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_traffic_stop_threshold = 1000;

        let orchestrator = Orchestrator::new_test(&config);

        // Set replication lag below threshold for every shard.
        {
            let publisher = orchestrator.publisher.lock().await;
            publisher.set_replication_lag(0, 500);
            publisher.set_replication_lag(1, 500);
        }

        let config = Arc::new(config);
        let mut waiter = Cutover::new_test(orchestrator, config);

        // Should exit immediately since lag (500) <= threshold (1000)
        let result = waiter.wait_for_replication().await;
        assert!(result.is_ok());

        // Maintenance mode should be on after wait_for_replication
        assert!(maintenance_mode::is_on(""));

        // Clean up maintenance mode
        maintenance_mode::stop(None);
        assert!(!maintenance_mode::is_on(""));
    }

    #[tokio::test]
    async fn test_wait_for_cutover_exits_when_lag_below_threshold() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_replication_lag_threshold = 100;
        config.config.general.cutover_timeout = 10000;

        let orchestrator = Orchestrator::new_test(&config);

        // Set replication lag below cutover threshold for every shard.
        {
            let publisher = orchestrator.publisher.lock().await;
            publisher.set_replication_lag(0, 50);
            publisher.set_replication_lag(1, 50);
        }

        let config = Arc::new(config);
        let mut waiter = Cutover::new_test(orchestrator, config);

        // should_cutover returns Lag when lag is below threshold
        let result = waiter.should_cutover(Duration::from_millis(100)).await;
        assert_eq!(result, CutoverAction::Go(CutoverReason::Lag));

        // Should exit immediately since lag (50) <= threshold (100)
        let result = waiter.wait_for_cutover().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_cutover_exits_when_last_transaction_old() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_replication_lag_threshold = 10;
        config.config.general.cutover_last_transaction_delay = 100;
        config.config.general.cutover_timeout = 10000;

        let orchestrator = Orchestrator::new_test(&config);

        {
            let publisher = orchestrator.publisher.lock().await;
            // Set lag above threshold so we don't exit on that condition
            publisher.set_replication_lag(0, 1000);
            // Set last_transaction to a time in the past (> 100ms ago)
            publisher.set_last_transaction(Some(Instant::now() - Duration::from_millis(200)));
        }

        let config = Arc::new(config);
        let mut waiter = Cutover::new_test(orchestrator, config);

        // should_cutover returns LastTransaction when last transaction is old
        let result = waiter.should_cutover(Duration::from_millis(100)).await;
        assert_eq!(result, CutoverAction::Go(CutoverReason::LastTransaction));

        // Should exit because last_transaction (200ms) > threshold (100ms)
        let result = waiter.wait_for_cutover().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_should_cutover_when_no_transaction() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_replication_lag_threshold = 10;
        config.config.general.cutover_last_transaction_delay = 100;
        config.config.general.cutover_timeout = 10000;

        let orchestrator = Orchestrator::new_test(&config);

        {
            let publisher = orchestrator.publisher.lock().await;
            // Set lag above threshold so we don't exit on that condition
            publisher.set_replication_lag(0, 1000);
            // No transaction set (None)
            publisher.set_last_transaction(None);
        }

        let config = Arc::new(config);
        let waiter = Cutover::new_test(orchestrator, config);

        // should_cutover returns LastTransaction when there's no transaction
        let result = waiter.should_cutover(Duration::from_millis(100)).await;
        assert_eq!(result, CutoverAction::Go(CutoverReason::LastTransaction));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_above_threshold_and_recent_transaction() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 10000;
        config.config.general.cutover_replication_lag_threshold = 100;
        config.config.general.cutover_last_transaction_delay = 500;

        let orchestrator = Orchestrator::new_test(&config);

        {
            let publisher = orchestrator.publisher.lock().await;
            // Lag above threshold
            publisher.set_replication_lag(0, 1000);
            // Recent transaction (50ms ago, threshold is 500ms)
            publisher.set_last_transaction(Some(Instant::now() - Duration::from_millis(50)));
        }

        let config = Arc::new(config);
        let waiter = Cutover::new_test(orchestrator, config);

        // Not timed out (100ms elapsed, timeout is 10000ms)
        let result = waiter.should_cutover(Duration::from_millis(100)).await;
        assert!(matches!(result, CutoverAction::NoGo { .. }));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_timeout_not_reached() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 1000;
        config.config.general.cutover_replication_lag_threshold = 10;
        config.config.general.cutover_last_transaction_delay = 500;

        let orchestrator = Orchestrator::new_test(&config);

        {
            let publisher = orchestrator.publisher.lock().await;
            // Lag above threshold
            publisher.set_replication_lag(0, 1000);
            // Recent transaction
            publisher.set_last_transaction(Some(Instant::now() - Duration::from_millis(100)));
        }

        let config = Arc::new(config);
        let waiter = Cutover::new_test(orchestrator, config);

        // Elapsed is 999ms, timeout is 1000ms - should not trigger timeout
        let result = waiter.should_cutover(Duration::from_millis(999)).await;
        assert!(matches!(result, CutoverAction::NoGo { .. }));
    }

    #[tokio::test]
    async fn test_should_not_cutover_when_lag_just_above_threshold() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 10000;
        config.config.general.cutover_replication_lag_threshold = 100;
        config.config.general.cutover_last_transaction_delay = 500;

        let orchestrator = Orchestrator::new_test(&config);

        {
            let publisher = orchestrator.publisher.lock().await;
            // Lag just above threshold (101 > 100)
            publisher.set_replication_lag(0, 101);
            // Recent transaction
            publisher.set_last_transaction(Some(Instant::now() - Duration::from_millis(50)));
        }

        let config = Arc::new(config);
        let waiter = Cutover::new_test(orchestrator, config);

        let result = waiter.should_cutover(Duration::from_millis(100)).await;
        assert!(matches!(result, CutoverAction::NoGo { .. }));
    }

    /// Cutover holds off until every shard has reported a lag measurement; an
    /// unreported shard reads as unknown (`None`), not zero.
    #[tokio::test]
    async fn should_not_cutover_before_every_shard_reports() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_timeout = 10000;
        config.config.general.cutover_replication_lag_threshold = 1000;
        config.config.general.cutover_last_transaction_delay = 500;

        // No shard has reported a lag yet.
        let orchestrator = Orchestrator::new_test(&config);
        // Recent transaction so only the lag arm decides the outcome.
        orchestrator
            .publisher
            .lock()
            .await
            .set_last_transaction(Some(Instant::now()));

        let config = Arc::new(config);
        let waiter = Cutover::new_test(orchestrator.clone(), config);
        let elapsed = Duration::from_millis(100);

        // Empty map: lag is unknown -> None, no cutover.
        assert_eq!(orchestrator.replication_lag().await, None);
        assert_matches!(
            waiter.should_cutover(elapsed).await,
            CutoverAction::NoGo { .. }
        );

        // Only shard 0 reported; shard 1 is missing, so the lag stays unknown.
        {
            let publisher = orchestrator.publisher.lock().await;
            publisher.set_replication_lag(0, 500);
        }
        assert_eq!(orchestrator.replication_lag().await, None);
        assert_matches!(
            waiter.should_cutover(elapsed).await,
            CutoverAction::NoGo { .. }
        );

        // Once every shard has a real, below-threshold measurement, it cuts over.
        orchestrator
            .publisher
            .lock()
            .await
            .set_replication_lag(1, 400);
        assert_eq!(
            waiter.should_cutover(elapsed).await,
            CutoverAction::Go(CutoverReason::Lag)
        );
    }

    /// Writes to a table outside the publication must not block cutover.
    /// Unrelated WAL advances the instance's LSN but not the publication's
    /// `confirmed_flush_lsn`; keepalives advance it to `wal_end` between
    /// transactions, so the drained slot reports ~0 lag and
    /// `wait_for_replication` completes.
    ///
    /// Runs against the live `pgdog` database (integration/setup.sh).
    #[tokio::test]
    async fn wait_for_replication_finishes_with_unrelated_writes() {
        use crate::backend::server::test::test_server;

        crate::logger();
        maintenance_mode::stop(None);

        const TRAFFIC_STOP: u64 = 1_000;

        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_traffic_stop_threshold = TRAFFIC_STOP;
        config.config.general.cutover_timeout = 120_000;

        let orchestrator = Orchestrator::new_test(&config);
        let publication = orchestrator.publication.clone();
        let slot = orchestrator.replication_slot().to_owned();
        let shards = orchestrator.source.shards().len();

        // Publication covers only `issue1_main`; `issue1_noise` is never published.
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

        orchestrator.source.launch();

        let stop = tokio_util::sync::CancellationToken::new();
        let mut publisher = orchestrator.publisher().await;
        let streams = publisher
            .prepare_replication(&orchestrator.source, &stop)
            .await
            .unwrap();
        let tasks: Vec<_> = streams
            .into_iter()
            .map(|stream| {
                let task = crate::api::replication::ReplicationSlotTask::new(
                    stream,
                    &orchestrator.source,
                    &orchestrator.destination,
                    stop.clone(),
                );
                publisher.track_replication(task.source_shard, task.replication.clone());
                crate::api::run_task(task)
            })
            .collect();
        drop(publisher);

        let config = Arc::new(config);
        let mut waiter = Cutover::new_test(orchestrator.clone(), config);

        // ~10MB of incompressible WAL into the unpublished table: the slot
        // decodes none of it, but the instance LSN advances, inflating lag.
        source
            .execute(
                "INSERT INTO issue1_noise \
                 SELECT g, (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 64)) \
                 FROM generate_series(1, 5000) g",
            )
            .await
            .unwrap();

        // Let one check_lag tick (1s) refresh the lag cache with the post-write
        // value before sampling the gate.
        safe_sleep(Duration::from_secs(1)).await;

        // 30s margin: keepalive cadence (wal_sender_timeout) is not bounded by
        // the 1s sleep, so give confirmed_flush_lsn room to advance.
        let result = safe_timeout(Duration::from_secs(20), waiter.wait_for_replication()).await;
        let maintenance_on = maintenance_mode::is_on("");

        // Clean up before asserting so a failure can't leak slots or maintenance mode.
        stop.cancel();
        let mut drained = Ok(());
        for task in tasks {
            drained = drained.and(task.await);
        }
        maintenance_mode::stop(None);
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
        // Cutover fired: once the lag fell below the threshold, traffic stopped.
        assert!(
            maintenance_on,
            "wait_for_replication returned without stopping traffic (cutover did not fire)"
        );
    }
}
