//! Replication task definitions and statuses: the migration, one cluster
//! stream of it, and one shard slot of that stream.

use std::fmt;

use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Databases, Lsn, MissedRows};

/// Direction of a replication task: the initial migration (`Forward`) or the
/// post-cutover reverse stream that backs a rollback (`Reverse`). A `CUTOVER`
/// on a `Reverse` task is therefore a rollback. Affects reported status only,
/// not control flow.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[display(rename_all = "snake_case")]
pub enum ReplicationDirection {
    #[default]
    Forward,
    Reverse,
}

/// Why the replication task stopped waiting and cut traffic over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationCutoverReason {
    /// Replication lag reached the configured threshold.
    #[display("lag")]
    Lag,
    /// No transaction was applied for the configured delay.
    #[display("last transaction")]
    LastTransaction,
    /// The configured wait expired before the other conditions were met.
    #[display("timeout")]
    Timeout,
}

/// The migration one replication task drives, including every cutover it
/// performs.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("replication {databases}")]
pub struct ReplicationDefinition {
    pub databases: Databases,
    pub auto_cutover: bool,
}

/// Stages of logical replication, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplicationStatus {
    /// Streaming changes to catch the destination up.
    #[display("replicating")]
    Replicating,
    #[display("stopping traffic")]
    StoppingTraffic,
    #[display("waiting for catch-up")]
    WaitingForCatchUp,
    #[display("syncing schema")]
    SyncingSchema,
    #[display("preparing reverse replication")]
    PreparingReverseReplication,
    /// Cutting traffic over to the destination.
    #[display("cutting over")]
    CuttingOver,
    /// Cutting traffic back to the original after a prior cutover (rollback).
    #[display("rolling back")]
    RollingBack,
    /// A stage this build does not know.
    #[display("")]
    #[serde(other)]
    Other,
}

/// The cluster one replication subtask streams until the parent task cuts
/// traffic over. `databases` always names the migration's original source and
/// destination; `direction` says which of them the changes flow from.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("replication {databases}{}", if matches!(direction, ReplicationDirection::Reverse) { " (reverse)" } else { "" })]
pub struct ReplicationClusterDefinition {
    pub databases: Databases,
    pub direction: ReplicationDirection,
}

/// Stages of one replication cluster, reported as the subtask's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplicationClusterStatus {
    #[display("initializing replication streams")]
    InitializingReplicationStreams,
    /// Streaming changes to catch the destination up.
    #[display("replicating, {progress}")]
    Replicating { progress: ReplicationProgress },
    /// Stopped streaming so the parent task can cut traffic over.
    #[display("stopped for cutover ({reason})")]
    StoppedForCutover { reason: ReplicationCutoverReason },
    /// A stage this build does not know.
    #[display("")]
    #[serde(other)]
    Other,
}

/// How far the whole cluster has replicated: the largest lag of its shards,
/// and how long ago the newest transaction was applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReplicationProgress {
    pub lag_bytes: Option<u64>,
    pub last_transaction_ms: Option<u64>,
}

impl fmt::Display for ReplicationProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.lag_bytes {
            Some(lag) => write!(f, "lag {lag} bytes")?,
            None => write!(f, "lag unknown")?,
        }
        if let Some(age) = self.last_transaction_ms {
            write!(f, ", last transaction {age}ms ago")?;
        }
        Ok(())
    }
}

/// The slot one per-shard replication subtask streams from.
#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize, JsonSchema)]
#[display("{slot} on {host}:{port}/{database_name}")]
pub struct ReplicationShardDefinition {
    pub slot: String,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    pub source_shard: usize,
}

/// How far one replication slot has streamed.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ReplicationShardStatus {
    pub lsn: Lsn,
    /// `pg_current_wal_lsn() - confirmed_flush_lsn`.
    pub lag_bytes: Option<i64>,
    pub missed_rows: MissedRows,
}

impl fmt::Display for ReplicationShardStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.lag_bytes {
            Some(b) => write!(f, "lag {} bytes at {}", b, self.lsn),
            None => write!(f, "lag unknown at {}", self.lsn),
        }
    }
}
