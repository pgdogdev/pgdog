use crate::{
    backend::{
        Cluster, Schema,
        databases::{cancel_all, cutover},
        maintenance_mode,
        schema::sync::{PgDump, pg_dump::PgDumpOutput},
    },
    util::{format_bytes, human_duration, random_string},
};
use pgdog_config::{ConfigAndUsers, CutoverTimeoutAction, RewriteMode};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::Mutex,
    time::{Instant, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Orchestrator {
    source: Cluster,
    destination: Cluster,
    publication: String,
    schema: Option<PgDumpOutput>,
    publisher: Arc<Mutex<Publisher>>,
    replication_slot: String,
}

/// A handle to a publication's replication slots, decoupled from the rest of
/// the orchestrator. Awaiting [`PublicationGuard::cleanup`] drops every slot
/// the publisher still owns — a no-op once `replicate` has handed them off to
/// the streaming tasks.
pub(crate) struct PublicationGuard {
    publisher: Arc<Mutex<Publisher>>,
}

impl PublicationGuard {
    /// Drop any replication slots the publisher still owns.
    pub(crate) async fn cleanup(self) -> Result<(), Error> {
        self.publisher.lock().await.cleanup().await
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
            schema: None,
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
    fn refresh_publisher(&mut self) {
        let publisher = Publisher::new(
            &self.publication,
            config().config.general.query_parser_engine,
            self.replication_slot.clone(),
        );
        self.publisher = Arc::new(Mutex::new(publisher));
    }

    pub(crate) fn replication_slot(&self) -> &str {
        &self.replication_slot
    }

    pub(crate) async fn load_schema(&mut self) -> Result<(), Error> {
        let pg_dump = PgDump::new(&self.source, &self.publication);
        let output = pg_dump.dump().await?;
        self.schema = Some(output);

        Ok(())
    }

    /// Schema getter.
    pub(crate) fn schema(&self) -> Result<&PgDumpOutput, Error> {
        self.schema.as_ref().ok_or(Error::NoSchema)
    }

    pub(crate) async fn schema_sync_pre(&mut self, ignore_errors: bool) -> Result<(), Error> {
        let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

        orchestrator_state(OrchestratorState::SchemSyncPre);

        schema
            .restore(&self.destination, ignore_errors, SyncState::PreData)
            .await?;

        // Schema changed on the destination.
        reload_from_existing()?;

        self.destination = databases().schema_owner(&self.destination.identifier().database)?;
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination.wait_schema_loaded().await;

        self.refresh_publisher();

        if self.destination.rewrite().primary_key == RewriteMode::RewriteOmni {
            // Install the sharded sequence and supporting schema
            // and functions.
            Schema::install(&self.destination).await?;
        }

        Ok(())
    }

    /// Remove any blockers for reverse replication.
    pub(crate) async fn schema_sync_post_cutover(
        &mut self,
        ignore_errors: bool,
    ) -> Result<(), Error> {
        let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

        orchestrator_state(OrchestratorState::SchemaSyncPostCutover);

        schema
            .restore(&self.destination, ignore_errors, SyncState::PostCutover)
            .await?;

        Ok(())
    }

    pub(crate) async fn data_sync(&self, cancel: &CancellationToken) -> Result<(), Error> {
        let mut publisher = self.publisher.lock().await;

        orchestrator_state(OrchestratorState::DataSync);
        // Run data sync for all tables in parallel using multiple replicas,
        // if available.
        publisher
            .data_sync(&self.source, &self.destination, cancel)
            .await?;

        Ok(())
    }

    /// Take a [`PublicationGuard`] over this orchestrator's replication slots.
    pub(crate) fn publication_guard(&self) -> PublicationGuard {
        PublicationGuard {
            publisher: self.publisher.clone(),
        }
    }

    /// Replicate forever.
    ///
    /// Useful for CLI interface only, since this will never stop.
    ///
    pub(crate) async fn replicate(&self) -> Result<ReplicationWaiter, Error> {
        let mut publisher = self.publisher.lock().await;
        let waiter = publisher.replicate(&self.source, &self.destination).await?;

        orchestrator_state(OrchestratorState::Replication);

        Ok(ReplicationWaiter {
            orchestrator: self.clone(),
            waiter,
            config: config(),
        })
    }

    pub(crate) async fn schema_sync_post(&mut self, ignore_errors: bool) -> Result<(), Error> {
        let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

        orchestrator_state(OrchestratorState::SchemaSyncPost);

        schema
            .restore(&self.destination, ignore_errors, SyncState::PostData)
            .await?;

        Ok(())
    }

    pub(crate) async fn schema_sync_cutover(&self, ignore_errors: bool) -> Result<(), Error> {
        // Sequences won't be used in a sharded database.
        let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

        orchestrator_state(OrchestratorState::SchemaSyncCutover);

        schema
            .restore(&self.destination, ignore_errors, SyncState::Cutover)
            .await?;

        Ok(())
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
pub struct ReplicationWaiter {
    orchestrator: Orchestrator,
    waiter: Waiter,
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

impl ReplicationWaiter {
    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        self.waiter.wait().await
    }

    pub(crate) fn stop(&self) {
        self.waiter.stop();
    }

    /// Wait for replication to catch up.
    async fn wait_for_replication(&mut self) -> Result<(), Error> {
        let traffic_stop = self.config.config.general.cutover_traffic_stop_threshold;

        info!(
            "[cutover] started, waiting for traffic stop threshold={}",
            format_bytes(traffic_stop)
        );

        // Check once a second how far we got.
        let mut check = interval(Duration::from_secs(1));

        loop {
            select! {
                _ = check.tick() => {}

                // In case replication breaks now.
                res = self.waiter.wait() => {
                    res?;
                }
            }

            let Some(lag) = self.orchestrator.replication_lag().await else {
                info!("[cutover] replication lag is not calculated for all shards, yet");
                continue;
            };

            cutover_state(CutoverState::WaitingForReplication { lag });
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
    async fn wait_for_cutover(&mut self) -> Result<(), Error> {
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
        let mut check = interval(Duration::from_millis(50));
        let mut log = interval(Duration::from_secs(1));
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

                // In case replication breaks now.
                res = self.waiter.wait() => {
                    ok_or_abort!(res);
                }
            }

            let elapsed = start.elapsed();
            let cutover_reason = self.should_cutover(elapsed).await;

            cutover_state(CutoverState::WaitForCutover {
                action: cutover_reason,
            });

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

    /// Perform traffic cutover between source and destination.
    ///
    /// The pre-switch wait (`wait_for_replication`, `wait_for_cutover`) is
    /// cancellable: a `STOP_TASK` (via `cancel`) there resumes traffic, stops
    /// the replication streams, and returns without moving any traffic. Past
    /// the point of no return the switch always runs to completion — cancelling
    /// it would leave traffic split between source and destination.
    pub(crate) async fn cutover(&mut self, cancel: &CancellationToken) -> Result<(), Error> {
        select! {
            // Nothing has moved yet (`wait_for_replication` only pauses traffic
            // at its very end). Resume traffic (no-op if never paused) and wind
            // the streams down, so the aborted cutover leaves nothing running.
            _ = cancel.cancelled() => {
                maintenance_mode::stop(None);
                self.waiter.stop();
                warn!("[cutover] stop requested before the traffic switch, aborting cutover");
                cutover_state(CutoverState::Abort {
                    error: "stopped before cutover".into(),
                });
                return Ok(());
            }
            res = async {
                self.wait_for_replication().await?;
                self.wait_for_cutover().await
            } => { res?; }
        }

        // We're going, point of no return.
        self.waiter.stop();
        ok_or_abort!(self.waiter.wait().await);
        ok_or_abort!(self.orchestrator.schema_sync_cutover(true).await);
        // Traffic is about to go to the new cluster.
        // If this fails, we'll resume traffic to the old cluster instead
        // and the whole thing needs to be done from scratch.
        ok_or_abort!(
            cutover(
                &self.orchestrator.source.identifier().database,
                &self.orchestrator.destination.identifier().database,
            )
            .await
        );

        // Source is now destination and vice versa; reload cluster refs and
        // create a fresh publisher for reverse replication.
        ok_or_abort!(self.orchestrator.refresh());
        self.orchestrator.refresh_publisher();

        info!("[cutover] setting up reverse replication");

        // Fix any reverse replication blockers.
        ok_or_abort!(self.orchestrator.schema_sync_post_cutover(true).await);

        // Create reverse replication in case we need to rollback.
        let waiter = ok_or_abort!(self.orchestrator.replicate().await);

        // Drive the running waiter as a background api task so it stays visible
        // in SHOW TASKS and can be cut over (rollback) or stopped.
        crate::api::run_task(
            crate::api::replication::ReplicationTask::builder()
                .waiter(waiter)
                .direction(crate::api::replication::Direction::Reverse)
                .build(),
        );

        // Slot is established and capturing — now safe to resume traffic.
        info!("[cutover] complete, resuming traffic");

        // Point traffic to the other database and resume.
        maintenance_mode::stop(None);

        cutover_state(CutoverState::Complete);

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
                cutover_state(CutoverState::Abort {
                    error: err.to_string(),
                });
                return Err(err.into());
            }
        }
    };
}

use ok_or_abort;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::Cluster;
    use pgdog_config::ConfigAndUsers;
    use std::assert_matches;
    use std::sync::Arc;
    use tokio::time::Instant;

    impl Orchestrator {
        fn new_test(config: &ConfigAndUsers) -> Self {
            let cluster = Cluster::new_test(config);
            let publication = "test_pub".to_owned();
            let replication_slot = "test_slot".to_owned();
            let publisher = Publisher::new(
                &publication,
                config.config.general.query_parser_engine,
                replication_slot.clone(),
            );
            Self {
                source: cluster.clone(),
                destination: cluster,
                publication,
                schema: None,
                publisher: Arc::new(Mutex::new(publisher)),
                replication_slot,
            }
        }
    }

    impl ReplicationWaiter {
        fn new_test(orchestrator: Orchestrator, config: Arc<ConfigAndUsers>) -> Self {
            Self {
                orchestrator,
                waiter: Waiter::new_test(),
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
        let mut waiter = ReplicationWaiter::new_test(orchestrator, config);

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
        let mut waiter = ReplicationWaiter::new_test(orchestrator, config);

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
        let mut waiter = ReplicationWaiter::new_test(orchestrator, config);

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
        let waiter = ReplicationWaiter::new_test(orchestrator, config);

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
        let waiter = ReplicationWaiter::new_test(orchestrator, config);

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
        let waiter = ReplicationWaiter::new_test(orchestrator, config);

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
        let waiter = ReplicationWaiter::new_test(orchestrator, config);

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
        let waiter = ReplicationWaiter::new_test(orchestrator.clone(), config);
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
        use tokio::time::{sleep, timeout};

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

        let stream = orchestrator
            .publisher
            .lock()
            .await
            .replicate(&orchestrator.source, &orchestrator.destination)
            .await
            .unwrap();

        let config = Arc::new(config);
        let mut waiter = ReplicationWaiter {
            orchestrator: orchestrator.clone(),
            waiter: stream,
            config,
        };

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
        sleep(Duration::from_secs(1)).await;

        // 30s margin: keepalive cadence (wal_sender_timeout) is not bounded by
        // the 1s sleep, so give confirmed_flush_lsn room to advance.
        let result = timeout(Duration::from_secs(30), waiter.wait_for_replication()).await;
        let maintenance_on = maintenance_mode::is_on("");

        // Clean up before asserting so a failure can't leak slots or maintenance mode.
        waiter.stop();
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
