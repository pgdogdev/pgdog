use crate::{
    backend::{
        databases::{cancel_all, cutover},
        maintenance_mode,
        schema::sync::{pg_dump::PgDumpOutput, PgDump},
        Cluster,
    },
    util::{format_bytes, human_duration, random_string},
};
use pgdog_config::CutoverTimeoutAction;
use std::{sync::Arc, time::Duration};
use tokio::{
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
        if self.destination.shards().len() > 1 {
            let schema = self.schema.as_ref().ok_or(Error::NoSchema)?;

            schema
                .restore(&self.destination, ignore_errors, SyncState::Cutover)
                .await?;
        }

        Ok(())
    }

    /// Get the largest replication lag out of all the shards.
    async fn replication_lag(&self) -> u64 {
        let lag = self.publisher.lock().await.replication_lag();
        lag.iter().map(|(_, lag)| *lag).max().unwrap_or_default() as u64
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
}

impl ReplicationWaiter {
    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        self.waiter.wait().await
    }

    pub(crate) fn stop(&self) {
        self.waiter.stop();
    }

    /// Perform traffic cutover between source and destination.
    pub(crate) async fn cutover(&mut self) -> Result<(), Error> {
        let config = config();
        let traffic_stop = config.config.general.cutover_traffic_stop_threshold;
        let cutover_threshold = config.config.general.cutover_replication_lag_threshold;
        let last_transaction_delay =
            Duration::from_millis(config.config.general.cutover_last_transaction_delay);
        let cutover_timeout = Duration::from_millis(config.config.general.cutover_timeout);
        let cutover_timeout_action = config.config.general.cutover_timeout_action;

        // Check once a second how far we got.
        let mut check = interval(Duration::from_secs(1));

        loop {
            check.tick().await;
            let lag = self.orchestrator.replication_lag().await;

            // Time to go.
            if lag <= traffic_stop {
                info!(
                    "[cutover] stopping traffic, lag={}, threshold={}",
                    format_bytes(lag),
                    format_bytes(config.config.general.cutover_traffic_stop_threshold),
                );

                // Pause traffic.
                maintenance_mode::start();

                // Cancel any running queries.
                cancel_all(&self.orchestrator.source.identifier().database).await?;

                break;
                // TODO: wait for clients to all stop.
            }
        }

        // Check more frequently.
        let mut check = interval(Duration::from_millis(50));
        // Abort clock starts now.
        let start = Instant::now();

        loop {
            check.tick().await;
            let cutover_timeout_exceeded = start.elapsed() >= cutover_timeout;

            if cutover_timeout_action == CutoverTimeoutAction::Abort {
                maintenance_mode::stop();
                warn!("[cutover] abort timeout reached, resuming traffic");
                return Err(Error::AbortTimeout);
            }

            let lag = self.orchestrator.replication_lag().await;
            let last_transaction = self
                .orchestrator
                .publisher
                .lock()
                .await
                .last_transaction()
                .unwrap_or_default();

            // Perform cutover if any of the following is true:
            //
            // 1. Cutover timeout exceeded and action is cutover.
            // 2. Replication lag is below threshold.
            // 3. Last transaction was a while ago.
            //
            let should_cutover = cutover_timeout_exceeded
                || lag <= cutover_threshold
                || last_transaction > last_transaction_delay;

            if should_cutover {
                info!(
                    "[cutover] starting cutover, lag={}, threshold={}, last_transaction={}, timeout={}",
                    format_bytes(lag),
                    format_bytes(cutover_threshold),
                    human_duration(last_transaction),
                    cutover_timeout_exceeded,
                );

                // We're going, point of no return.
                self.orchestrator.publisher.lock().await.request_stop();
                ok_or_abort!(self.waiter.wait().await);
                ok_or_abort!(self.orchestrator.schema_sync_cutover(true).await);
                // Traffic is about to go to the new cluster.
                // If this fails, we'll resume traffic to the old cluster instead
                // and the whole thing needs to be done from scratch.
                ok_or_abort!(cutover(
                    &self.orchestrator.source.identifier().database,
                    &self.orchestrator.destination.identifier().database,
                ));

                info!("[cutover] complete, resuming traffic");

                // Point traffic to the other database and resume.
                maintenance_mode::stop();

                info!("[cutover] stopping replication");

                info!("[cutover] replication stopped");

                break;
            }
        }

        Ok(())
    }
}

macro_rules! ok_or_abort {
    ($expr:expr) => {
        match $expr {
            Ok(_) => (),
            Err(err) => {
                maintenance_mode::stop();
                return Err(err.into());
            }
        }
    };
}

use ok_or_abort;
