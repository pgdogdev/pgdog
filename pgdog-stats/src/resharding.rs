use std::time::SystemTime;

use derive_more::Display;
use pgdog_config::ServerAuth;
use serde::{Deserialize, Serialize};

use crate::{Lsn, TaskId, User};

/// Replication slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSlot {
    pub name: String,
    pub lsn: Lsn,
    pub lag: i64,
    pub copy_data: bool,
    pub address: Address,
    pub last_transaction: Option<SystemTime>,
    pub task_id: Option<TaskId>,
}

/// Server address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, Eq, Hash)]
pub struct Address {
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// PostgreSQL database name.
    pub database_name: String,
    /// Username.
    pub user: String,
    /// Password.
    pub passwords: Vec<String>,
    /// Server auth mode for backend connections.
    #[serde(default)]
    pub server_auth: ServerAuth,
    /// Optional IAM region override.
    pub server_iam_region: Option<String>,
    /// Database number (in the config).
    pub database_number: usize,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq, Serialize, Deserialize)]
pub struct SchemaStatement {
    pub id: i64,
    pub user: User,
    pub shard: usize,
    pub sql: String,
    pub kind: StatementKind,
    pub sync_state: SyncState,
    pub started_at: Option<SystemTime>,
    pub table_schema: Option<String>,
    pub table_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq, Serialize, Deserialize)]
pub struct SchemaStatementTask {
    pub statement: SchemaStatement,
    pub running: bool,
    pub done: bool,
    pub error: Option<String>,
}

#[derive(
    Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema,
)]
#[display(rename_all = "snake_case")]
pub enum StatementKind {
    Table,
    Index,
    Statement,
}

#[derive(
    Debug, Display, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema,
)]
#[display(rename_all = "snake_case")]
pub enum SyncState {
    PreData,
    PostData,
    Cutover,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MissedRows {
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
}

impl MissedRows {
    pub fn non_zero(&self) -> bool {
        self.inserts > 0 || self.updates > 0 || self.deletes > 0
    }

    pub fn merge(&mut self, other: Self) {
        self.inserts += other.inserts;
        self.updates += other.updates;
        self.deletes += other.deletes;
    }

    pub fn record(&mut self, tag: &str) {
        if tag.starts_with("INSERT") {
            self.inserts += 1;
        } else if tag.starts_with("UPDATE") {
            self.updates += 1;
        } else if tag.starts_with("DELETE") {
            self.deletes += 1;
        }
    }
}

impl std::fmt::Display for MissedRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut written = false;
        if self.inserts > 0 {
            write!(f, "insert={}", self.inserts)?;
            written = true;
        }
        if self.updates > 0 {
            write!(
                f,
                "{}update={}",
                if written { " " } else { "" },
                self.updates
            )?;
            written = true;
        }
        if self.deletes > 0 {
            write!(
                f,
                "{}delete={}",
                if written { " " } else { "" },
                self.deletes
            )?;
        }
        Ok(())
    }
}
