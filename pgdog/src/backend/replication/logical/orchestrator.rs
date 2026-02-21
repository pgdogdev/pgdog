use crate::{
    backend::{
        databases::cutover,
        maintenance_mode,
        schema::sync::{pg_dump::PgDumpOutput, PgDump},
        Cluster,
    },
    util::{format_bytes, random_string},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::interval};
use tracing::{error, info};

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
    pub(crate) async fn replicate(&self) -> Result<Waiter, Error> {
        let mut publisher = self.publisher.lock().await;
        publisher.replicate(&self.destination).await
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

        // Start replication to catch up.
        let mut waiter = self.replicate().await?;

        // Check once a second how far we got.
        let mut check = interval(Duration::from_secs(1));
        // Ready for cutover.
        let mut paused = false;

        let config = config();

        loop {
            check.tick().await;
            let lag = self.publisher.lock().await.replication_lag();

            for (shard, lag) in lag.iter() {
                info!("[cutover] replication lag={}, shard={}", lag, shard);
            }

            let max_lag = lag.iter().map(|(_, lag)| *lag).max().unwrap_or_default() as u64;

            // Time to go.
            if max_lag <= config.config.general.cutover_traffic_stop_threshold && !paused {
                info!(
                    "[cutover] stopping traffic, lag={}, threshold={}",
                    format_bytes(max_lag),
                    format_bytes(config.config.general.cutover_traffic_stop_threshold),
                );
                // Pause traffic.
                maintenance_mode::start();
                paused = true;
                // TODO: wait for clients to all stop.
            }

            // Okay lets go.
            // TODO: will lag ever be zero? We want to check
            // that no data changes have been sent in over a second or something
            // like that.
            // TODO: add timeout.
            if max_lag <= config.config.general.cutover_replication_lag_threshold && paused {
                info!(
                    "[cutover] starting cutover, lag={}, threshold={}",
                    format_bytes(max_lag),
                    format_bytes(config.config.general.cutover_replication_lag_threshold)
                );

                self.publisher.lock().await.request_stop();
                let result = waiter.wait().await;

                match result {
                    Ok(_) => (),
                    Err(err) => {
                        maintenance_mode::stop();
                        return Err(err);
                    }
                }

                info!("[cutover] replication terminated, performing configuration reload");

                // No matter what happens, resume traffic.
                let result = self.cutover(true).await;

                match &result {
                    Ok(()) => {
                        info!("[cutover] cutover complete, resuming traffic");
                    }

                    Err(err) => {
                        error!("[cutover] cutover failed, resuming traffic, error: {}", err);
                    }
                }

                maintenance_mode::stop();

                result?;
                break;
            }
        }

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

    /// Perform cutover.
    pub(crate) async fn cutover(&self, ignore_errors: bool) -> Result<(), Error> {
        self.schema_sync_cutover(ignore_errors).await?;

        // Immediate traffic cutover in-memory.
        // N.B. Make sure to write new config to disk.
        cutover(
            &self.source.identifier().database,
            &self.destination.identifier().database,
        )?;

        Ok(())
    }

    pub(crate) async fn cleanup(&mut self) -> Result<(), Error> {
        let mut guard = self.publisher.lock().await;
        guard.cleanup().await?;

        Ok(())
    }
}
