//! Schema sync between two databases.

use std::sync::Arc;

use pgdog_config::RewriteMode;
use tracing::{info, warn};

use crate::backend::{
    self, Cluster, Schema,
    databases::{databases, reload_from_existing},
    pool::{Guard, Request},
    schema::sync::{PgDump, SchemaSyncError, Statement, pg_dump::PgDumpOutput},
};

/// Sync the schema from a source database to a destination, one phase at a
/// time.
#[derive(Debug)]
pub(crate) struct SchemaSync {
    source: Cluster,
    destination: Cluster,
    publication: String,
}

impl SchemaSync {
    pub(crate) fn new(
        source: &str,
        destination: &str,
        publication: &str,
    ) -> Result<Self, SchemaSyncError> {
        Ok(Self {
            source: databases().schema_owner(source)?,
            destination: databases().schema_owner(destination)?,
            publication: publication.to_owned(),
        })
    }

    /// Re-resolve both ends from the live databases registry, after something
    /// (a schema sync of our own, a DDL reload, a cutover) reloaded the pools.
    fn refresh(&mut self) -> Result<(), SchemaSyncError> {
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination = databases().schema_owner(&self.destination.identifier().database)?;

        Ok(())
    }

    /// Dump the source schema.
    pub(crate) async fn dump(&self) -> Result<Arc<PgDumpOutput>, SchemaSyncError> {
        Ok(Arc::new(
            PgDump::new(&self.source, &self.publication).dump().await?,
        ))
    }

    /// Take a connection to one destination shard, to apply statements through.
    pub(crate) async fn shard(&self, shard: usize) -> Result<ShardRestore, SchemaSyncError> {
        let primary = self.destination.primary(shard, &Request::default()).await?;

        info!(
            "syncing schema into shard {} [{}, {}]",
            shard,
            primary.addr(),
            self.destination.name()
        );

        Ok(ShardRestore { primary })
    }

    /// Pick up the destination's new schema after a pre-data phase: its pools
    /// reload, which invalidates our cluster refs.
    pub(crate) async fn reload_destination(&mut self) -> Result<(), SchemaSyncError> {
        reload_from_existing()?;
        self.refresh()?;
        self.destination.wait_ready().await;

        if self.destination.rewrite().primary_key == RewriteMode::RewriteOmni {
            Schema::install(&self.destination).await?;
        }

        Ok(())
    }

    pub(crate) fn shards(&self) -> usize {
        self.destination.shards().len()
    }
}

/// A connection to one destination shard, applying statements one at a time.
#[derive(Debug)]
pub(crate) struct ShardRestore {
    primary: Guard,
}

impl ShardRestore {
    /// Apply one statement. An "already exists" error is reported as
    /// [`StatementOutcome::Skipped`] when the statement tolerates it, and any
    /// other error fails the restore unless `ignore_errors` is set.
    pub(crate) async fn execute(
        &mut self,
        statement: &Statement,
        ignore_errors: bool,
    ) -> Result<StatementOutcome, SchemaSyncError> {
        let Err(err) = self.primary.execute(&statement.sql).await else {
            return Ok(StatementOutcome::Applied);
        };

        let backend::Error::ExecutionError(execution_error) = &err else {
            return Err(err.into());
        };

        if statement.skip_if_exists
            && matches!(
                execution_error.code.as_str(),
                "42P16" | "42710" | "42809" | "42P07"
            )
        {
            warn!("entity already exists, skipping");
            return Ok(StatementOutcome::Skipped);
        }

        if !ignore_errors {
            return Err(err.into());
        }

        warn!("skipping: {}", err);

        Ok(StatementOutcome::Failed(err.to_string()))
    }
}

/// What one statement did on one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatementOutcome {
    Applied,
    Skipped,
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::config;

    async fn destination() -> ShardRestore {
        let cluster = Cluster::new_test_single_shard(&config());
        cluster.launch();

        ShardRestore {
            primary: cluster.primary(0, &Request::default()).await.unwrap(),
        }
    }

    impl ShardRestore {
        async fn apply(&mut self, sql: &str) -> Result<StatementOutcome, SchemaSyncError> {
            self.execute(&Statement::new(sql), false).await
        }

        async fn replay(&mut self, sql: &str) -> Result<StatementOutcome, SchemaSyncError> {
            self.execute(&Statement::new(sql).set_skip_if_exists(), false)
                .await
        }

        async fn force(&mut self, sql: &str) -> Result<StatementOutcome, SchemaSyncError> {
            self.execute(&Statement::new(sql), true).await
        }
    }

    /// A resumed phase replays statements the destination already has. Each
    /// shape answers with a different already-exists SQLSTATE, and the same
    /// replay lands differently under each option: tolerated it is skipped,
    /// strict it fails, and `ignore_errors` records the failure.
    #[tokio::test]
    async fn one_replay_under_every_option() {
        let mut destination = destination().await;
        let replays = [
            (
                "DROP TABLE IF EXISTS _sync_table",
                "CREATE TABLE _sync_table (id BIGINT)",
                "CREATE TABLE _sync_table (id BIGINT)",
            ),
            (
                "DROP TYPE IF EXISTS _sync_type",
                "CREATE TYPE _sync_type AS ENUM ('a')",
                "CREATE TYPE _sync_type AS ENUM ('a')",
            ),
            (
                "DROP TABLE IF EXISTS _sync_pkey",
                "CREATE TABLE _sync_pkey (id BIGINT PRIMARY KEY)",
                "ALTER TABLE _sync_pkey ADD PRIMARY KEY (id)",
            ),
            (
                "DROP SEQUENCE IF EXISTS _sync_sequence",
                "CREATE SEQUENCE _sync_sequence",
                "ALTER TABLE _sync_sequence ADD COLUMN val INT",
            ),
        ];

        for (drop, create, replay) in replays {
            destination.force(drop).await.unwrap();

            assert_eq!(
                destination.apply(create).await.unwrap(),
                StatementOutcome::Applied,
                "{create}"
            );
            assert_eq!(
                destination.replay(replay).await.unwrap(),
                StatementOutcome::Skipped,
                "{replay}"
            );
            assert!(destination.apply(replay).await.is_err(), "{replay}");
            assert!(
                matches!(
                    destination.force(replay).await.unwrap(),
                    StatementOutcome::Failed(_)
                ),
                "{replay}"
            );

            destination.force(drop).await.unwrap();
        }
    }

    /// Tolerance is per SQLSTATE, not blanket, and `ignore_errors` keeps what
    /// the destination said so the shard cursor can show it.
    #[tokio::test]
    async fn an_unrelated_error_is_not_skipped() {
        let mut destination = destination().await;
        let bad = "CREATE TABLE _sync_bad (id _sync_no_such_type)";

        assert!(destination.replay(bad).await.is_err());

        let StatementOutcome::Failed(message) = destination.force(bad).await.unwrap() else {
            panic!("expected a recorded failure");
        };
        assert!(message.contains("_sync_no_such_type"), "got {message:?}");
    }

    /// One rejected statement must not strand the rest of the phase.
    #[tokio::test]
    async fn the_connection_survives_a_rejected_statement() {
        let mut destination = destination().await;

        for _ in 0..3 {
            assert!(
                destination
                    .apply("SELECT _sync_no_such_fn()")
                    .await
                    .is_err()
            );
        }

        assert_eq!(
            destination.apply("SELECT 1").await.unwrap(),
            StatementOutcome::Applied
        );
    }
}
