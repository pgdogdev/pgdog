//! Schema-sync task definitions and statuses, for the phase and its shards.

use std::fmt;
use std::sync::Arc;

use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Databases, SyncState};

/// The schema sync one schema-sync task runs, and at which stage.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("schema_sync({sync_state}) {databases}")]
pub struct SchemaSyncDefinition {
    pub databases: Databases,
    pub sync_state: SyncState,
    pub ignore_errors: bool,
    pub dry_run: bool,
}

/// One statement of a schema sync phase.
#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{sql}")]
pub struct SchemaSyncStatement {
    pub sql: String,
    /// The statement tolerates an "already exists" error from Postgres.
    pub skip_if_exists: bool,
}

impl SchemaSyncStatement {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            skip_if_exists: false,
        }
    }

    pub fn set_skip_if_exists(mut self) -> Self {
        self.skip_if_exists = true;
        self
    }
}

/// Status of a schema sync. The phase it applies lives on
/// [`SchemaSyncDefinition`]. The plan is reported once, by the parent task.
/// Each shard subtask reports a cursor into it as a [`SchemaShardStatus`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SchemaSyncStatus {
    /// Dumping the schema from the source.
    #[default]
    #[display("loading schema")]
    LoadingSchema,
    /// The dump is loaded and the phase's statements are known.
    #[display("applying {} statements", statements.len())]
    ApplyingStatements {
        #[serde(default)]
        statements: Arc<Vec<SchemaSyncStatement>>,
    },
    /// A status this build does not know.
    #[display("")]
    #[serde(other)]
    Other,
}

/// The destination shard one schema-sync subtask restores into.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("shard {shard} of {databases} ({sync_state})")]
pub struct SchemaShardDefinition {
    pub shard: u64,
    pub databases: Databases,
    pub sync_state: SyncState,
}

/// A statement one shard could not apply. `index` points into the plan the
/// parent task reported, and `message` is the error Postgres returned. The
/// display is one-based, to match the statement counter in the logs.
#[derive(Debug, Clone, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[display("statement {}: {message}", index + 1)]
pub struct SchemaStatementFailure {
    pub index: u64,
    pub message: String,
}

/// How far one destination shard got through the phase's statements. `applied`
/// counts the statements this shard ran, and `failures` records the ones it
/// could not.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct SchemaShardStatus {
    pub shard: u64,
    pub total: u64,
    pub applied: u64,
    pub skipped: u64,
    pub failures: Vec<SchemaStatementFailure>,
}

impl SchemaShardStatus {
    pub fn new(shard: u64, total: u64) -> Self {
        Self {
            shard,
            total,
            ..Default::default()
        }
    }

    pub fn done(&self) -> u64 {
        self.applied + self.skipped + self.failures.len() as u64
    }
}

impl fmt::Display for SchemaShardStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shard {}: {}/{} statements",
            self.shard,
            self.done(),
            self.total
        )?;

        if self.skipped > 0 {
            write!(f, ", {} skipped", self.skipped)?;
        }
        if !self.failures.is_empty() {
            write!(f, ", {} failed", self.failures.len())?;
        }

        Ok(())
    }
}
