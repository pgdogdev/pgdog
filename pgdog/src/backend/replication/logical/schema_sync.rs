//! Schema sync between two databases.

use std::sync::Arc;

use futures::future::join_all;
use pgdog_config::RewriteMode;
use tracing::{info, warn};

use crate::backend::{
    self, Cluster, Schema,
    databases::{databases, reload_from_existing},
    pool::{Guard, LbRequest},
    schema::sync::{PgDump, SchemaSyncError, Statement, pg_dump::PgDumpOutput},
};

use super::publisher::queries::server_version;

/// Sync the schema from a source database to a destination, one phase at a
/// time.
#[derive(Debug)]
pub(crate) struct SchemaSync {
    source: Cluster,
    destination: Cluster,
    publication: String,
    publication_tables_only: bool,
}

impl SchemaSync {
    pub(crate) fn new(
        source: &str,
        destination: &str,
        publication: &str,
        publication_tables_only: bool,
    ) -> Result<Self, SchemaSyncError> {
        Ok(Self {
            source: databases().schema_owner(source)?,
            destination: databases().schema_owner(destination)?,
            publication: publication.to_owned(),
            publication_tables_only,
        })
    }

    /// Re-resolve both ends from the live databases registry, after something
    /// (a schema sync of our own, a DDL reload, a cutover) reloaded the pools.
    fn refresh(&mut self) -> Result<(), SchemaSyncError> {
        self.source = databases().schema_owner(&self.source.identifier().database)?;
        self.destination = databases().schema_owner(&self.destination.identifier().database)?;

        Ok(())
    }

    /// Dump the source schema for the oldest known destination version.
    ///
    /// PostgreSQL 17 rejects NOT VALID foreign keys on partitioned tables:
    /// <https://www.postgresql.org/docs/17/sql-altertable.html#SQL-ALTERTABLE-DESC-ADD-TABLE-CONSTRAINT>.
    /// PostgreSQL 18 adds support:
    /// <https://www.postgresql.org/docs/18/release-18.html#RELEASE-18-CONSTRAINTS>.
    /// Keep normal constraint validation for older destinations.
    pub(crate) async fn dump(&self) -> Result<Arc<PgDumpOutput>, SchemaSyncError> {
        let destination_version = join_all((0..self.destination.shards().len()).map(
            |shard| async move {
                let mut primary = self
                    .destination
                    .primary(shard, &LbRequest::default())
                    .await
                    .ok()?;
                server_version(&mut primary).await.ok().flatten()
            },
        ))
        .await
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(0);
        Ok(Arc::new(
            PgDump::new(
                &self.source,
                &self.publication,
                self.publication_tables_only,
            )
            .dump(destination_version)
            .await?,
        ))
    }

    /// Take a connection to one destination shard, to apply statements through.
    pub(crate) async fn shard(&self, shard: usize) -> Result<ShardRestore, SchemaSyncError> {
        let primary = self
            .destination
            .primary(shard, &LbRequest::default())
            .await?;

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

/// `invalid_table_definition`: `ADD PRIMARY KEY` on a table that already has
/// one ("multiple primary keys for table are not allowed").
const INVALID_TABLE_DEFINITION: &str = "42P16";

/// `duplicate_object`: a constraint, trigger, type, collation, cast, policy,
/// rule, statistics object, operator class or family, conversion, access
/// method, event trigger, foreign-data wrapper, server or user mapping that
/// already exists.
const DUPLICATE_OBJECT: &str = "42710";

/// `duplicate_function`: `CREATE OPERATOR` for an operator that already
/// exists.
const DUPLICATE_FUNCTION: &str = "42723";

/// `wrong_object_type`: `ATTACH PARTITION` of a table that is already a
/// partition ("is already a partition"), or an `ALTER TABLE` on a relation
/// that already exists with another kind, for example a sequence.
const WRONG_OBJECT_TYPE: &str = "42809";

/// `duplicate_table`: a relation that already exists. This covers tables,
/// foreign tables, the index behind a `UNIQUE` or `EXCLUDE` constraint, and
/// `ALTER TABLE … INHERIT` that was already applied.
const DUPLICATE_TABLE: &str = "42P07";

/// A connection to one destination shard, applying statements one at a time.
#[derive(Debug)]
pub(crate) struct ShardRestore {
    primary: Guard,
}

impl ShardRestore {
    /// Apply one statement. An "already exists" error is reported as
    /// [`StatementOutcome::Skipped`] when the statement tolerates it, and any
    /// other error fails the restore unless `ignore_errors` is set for the
    /// restore or for the statement.
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
                INVALID_TABLE_DEFINITION
                    | DUPLICATE_OBJECT
                    | DUPLICATE_FUNCTION
                    | WRONG_OBJECT_TYPE
                    | DUPLICATE_TABLE
            )
        {
            warn!("entity already exists, skipping");
            return Ok(StatementOutcome::Skipped);
        }

        if !ignore_errors && !statement.ignore_errors {
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
            primary: cluster.primary(0, &LbRequest::default()).await.unwrap(),
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
            (
                "DROP OPERATOR IF EXISTS === (int4, int4)",
                "CREATE OPERATOR === (leftarg = int4, rightarg = int4, function = int4eq)",
                "CREATE OPERATOR === (leftarg = int4, rightarg = int4, function = int4eq)",
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
