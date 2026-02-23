use crate::{
    backend::{
        databases::{cancel_all, cutover},
        maintenance_mode,
        schema::sync::{pg_dump::PgDumpOutput, PgDump},
        Cluster,
    },
    util::{format_bytes, human_duration, random_string},
};
use pgdog_config::{ConfigAndUsers, CutoverTimeoutAction};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::Mutex,
    time::{interval, Instant},
};
use tracing::{info, warn};

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

    fn refresh(&mut self) -> Result<(), Error> {
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination = databases().schema_owner(&self.destination.identifier().database)?;
        self.refresh_publisher();

        Ok(())
    }

    fn refresh_publisher(&mut self) {
        let publisher = Publisher::new(
            &self.source,
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

        schema
            .restore(&self.destination, ignore_errors, SyncState::PreData)
            .await?;

        // Schema changed on the destination.
        reload_from_existing()?;

        self.destination = databases().schema_owner(&self.destination.identifier().database)?;
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination.wait_schema_loaded().await;

        self.refresh_publisher();

        Ok(())
    }

    /// Remove any blockers for reverse replication.
    pub(crate) async fn schema_sync_post_cutover(
        &mut self,
        ignore_errors: bool,
    ) -> Result<(), Error> {
        let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

        schema
            .restore(&self.destination, ignore_errors, SyncState::PostCutover)
            .await?;

        Ok(())
    }

    pub(crate) async fn data_sync(&self) -> Result<(), Error> {
        let mut publisher = self.publisher.lock().await;

        // Run data sync for all tables in parallel using multiple replicas,
        // if available.
        publisher.data_sync(&self.destination).await?;

        Ok(())
    }

    /// Replicate forever.
    ///
    /// Useful for CLI interface only, since this will never stop.
    ///
    pub(crate) async fn replicate(&self) -> Result<ReplicationWaiter, Error> {
        let mut publisher = self.publisher.lock().await;
        let waiter = publisher.replicate(&self.destination).await?;
        Ok(ReplicationWaiter {
            orchestrator: self.clone(),
            waiter,
            config: config(),
        })
    }

    /// Request replication stop.
    pub(crate) async fn request_stop(&self) {
        self.publisher.lock().await.request_stop();
    }

    /// Perform the entire flow in one swoop.
    pub(crate) async fn replicate_and_cutover(&mut self) -> Result<(), Error> {
        // Load the schema from source.
        self.load_schema().await?;

        // Sync the schema to destination.
        self.schema_sync_pre(true).await?;

        // Sync the data to destination.
        self.data_sync().await?;

        // Create secondary indexes on destination.
        self.schema_sync_post(true).await?;

        // Start replication to catch up and cutover once done.
        self.replicate().await?.cutover().await?;

        Ok(())
    }

    pub(crate) async fn schema_sync_post(&mut self, ignore_errors: bool) -> Result<(), Error> {
        let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

        schema
            .restore(&self.destination, ignore_errors, SyncState::PostData)
            .await?;

        Ok(())
    }

    pub(crate) async fn schema_sync_cutover(&self, ignore_errors: bool) -> Result<(), Error> {
        // Sequences won't be used in a sharded database.
        let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

        schema
            .restore(&self.destination, ignore_errors, SyncState::Cutover)
            .await?;

        Ok(())
    }

    /// Get the largest replication lag out of all the shards.
    async fn replication_lag(&self) -> u64 {
        let lag = self.publisher.lock().await.replication_lag();
        lag.values().copied().max().unwrap_or_default() as u64
    }

    pub(crate) async fn cleanup(&mut self) -> Result<(), Error> {
        let mut guard = self.publisher.lock().await;
        guard.cleanup().await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ReplicationWaiter {
    orchestrator: Orchestrator,
    waiter: Waiter,
    config: Arc<ConfigAndUsers>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CutoverReason {
    Lag,
    Timeout,
    LastTransaction,
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

            let lag = self.orchestrator.replication_lag().await;

            info!("[cutover] replication lag: {}", format_bytes(lag as u64));

            // Time to go.
            if lag <= traffic_stop {
                info!(
                    "[cutover] stopping traffic, lag={}, threshold={}",
                    format_bytes(lag),
                    format_bytes(traffic_stop),
                );

                // Pause traffic.
                maintenance_mode::start();

                // Cancel any running queries.
                cancel_all(&self.orchestrator.source.identifier().database).await?;

                break;
                // TODO: wait for clients to all stop.
            }
        }

        Ok(())
    }

    async fn should_cutover(&self, elapsed: Duration) -> Option<CutoverReason> {
        let cutover_timeout = Duration::from_millis(self.config.config.general.cutover_timeout);
        let cutover_threshold = self.config.config.general.cutover_replication_lag_threshold;
        let last_transaction_delay =
            Duration::from_millis(self.config.config.general.cutover_last_transaction_delay);

        let lag = self.orchestrator.replication_lag().await;
        let last_transaction = self.orchestrator.publisher.lock().await.last_transaction();
        let cutover_timeout_exceeded = elapsed >= cutover_timeout;

        if cutover_timeout_exceeded {
            Some(CutoverReason::Timeout)
        } else if lag <= cutover_threshold {
            Some(CutoverReason::Lag)
        } else if last_transaction.is_none_or(|t| t > last_transaction_delay) {
            Some(CutoverReason::LastTransaction)
        } else {
            None
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

        loop {
            select! {
                _ = check.tick() => {}

                _ = log.tick() => {
                    info!("[cutover] lag={}, last_transaction={}, timeout={}",
                        human_duration(cutover_timeout),
                        human_duration(last_transaction_delay),
                        format_bytes(cutover_threshold)
                    );
                }

                // In case replication breaks now.
                res = self.waiter.wait() => {
                    res?;
                }
            }

            let elapsed = start.elapsed();
            let cutover_reason = self.should_cutover(elapsed).await;
            match cutover_reason {
                Some(CutoverReason::Timeout) => {
                    if cutover_timeout_action == CutoverTimeoutAction::Abort {
                        maintenance_mode::stop();
                        warn!("[cutover] abort timeout reached, resuming traffic");
                        return Err(Error::AbortTimeout);
                    }
                }

                None => continue,
                Some(reason) => {
                    info!("[cutover] performing cutover now, reason: {}", reason);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Perform traffic cutover between source and destination.
    pub(crate) async fn cutover(&mut self) -> Result<(), Error> {
        self.wait_for_replication().await?;
        self.wait_for_cutover().await?;

        // We're going, point of no return.
        self.orchestrator.publisher.lock().await.request_stop();
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

        // Source is now destination and vice versa.
        ok_or_abort!(self.orchestrator.refresh());

        info!("[cutover] setting up reverse replication");

        // Fix any reverse replication blockers.
        ok_or_abort!(self.orchestrator.schema_sync_post_cutover(true).await);

        // Create reverse replication in case we need to rollback.
        let waiter = ok_or_abort!(self.orchestrator.replicate().await);

        // Let it run in the background.
        AsyncTasks::insert(TaskType::Replication(Box::new(waiter)));

        // It's not safe to resume traffic.
        info!("[cutover] complete, resuming traffic");

        // Point traffic to the other database and resume.
        maintenance_mode::stop();

        Ok(())
    }
}

macro_rules! ok_or_abort {
    ($expr:expr) => {
        match $expr {
            Ok(res) => res,
            Err(err) => {
                maintenance_mode::stop();
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
    use std::sync::Arc;
    use tokio::time::Instant;

    impl Orchestrator {
        fn new_test(config: &ConfigAndUsers) -> Self {
            let cluster = Cluster::new_test(config);
            Self {
                source: cluster.clone(),
                destination: cluster,
                publication: "test_pub".to_owned(),
                schema: None,
                publisher: Arc::new(Mutex::new(Publisher::default())),
                replication_slot: "test_slot".to_owned(),
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
        maintenance_mode::stop();
        assert!(!maintenance_mode::is_on());

        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_traffic_stop_threshold = 1000;

        let orchestrator = Orchestrator::new_test(&config);

        // Set replication lag below threshold
        orchestrator
            .publisher
            .lock()
            .await
            .set_replication_lag(0, 500);

        let config = Arc::new(config);
        let mut waiter = ReplicationWaiter::new_test(orchestrator, config);

        // Should exit immediately since lag (500) <= threshold (1000)
        let result = waiter.wait_for_replication().await;
        assert!(result.is_ok());

        // Maintenance mode should be on after wait_for_replication
        assert!(maintenance_mode::is_on());

        // Clean up maintenance mode
        maintenance_mode::stop();
        assert!(!maintenance_mode::is_on());
    }

    #[tokio::test]
    async fn test_wait_for_cutover_exits_when_lag_below_threshold() {
        let mut config = ConfigAndUsers::default();
        config.config.general.cutover_replication_lag_threshold = 100;
        config.config.general.cutover_timeout = 10000;

        let orchestrator = Orchestrator::new_test(&config);

        // Set replication lag below cutover threshold
        orchestrator
            .publisher
            .lock()
            .await
            .set_replication_lag(0, 50);

        let config = Arc::new(config);
        let mut waiter = ReplicationWaiter::new_test(orchestrator, config);

        // should_cutover returns Lag when lag is below threshold
        let result = waiter.should_cutover(Duration::from_millis(100)).await;
        assert_eq!(result, Some(CutoverReason::Lag));

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
        assert_eq!(result, Some(CutoverReason::LastTransaction));

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
        assert_eq!(result, Some(CutoverReason::LastTransaction));
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
        assert_eq!(result, None);
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
        assert_eq!(result, None);
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
        assert_eq!(result, None);
    }
}
