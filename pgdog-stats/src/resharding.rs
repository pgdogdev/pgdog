use std::time::SystemTime;

use derive_more::Display;
use pgdog_config::ServerAuth;
use serde::{Deserialize, Serialize};

use crate::{Lsn, User};

/// Replication slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSlot {
    pub name: String,
    pub lsn: Lsn,
    pub lag: i64,
    pub copy_data: bool,
    pub address: Address,
    pub last_transaction: Option<SystemTime>,
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
