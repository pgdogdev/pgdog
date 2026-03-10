use std::{sync::Arc, time::SystemTime};

use pgdog_config::ServerAuth;
use serde::{Deserialize, Serialize};

use crate::Lsn;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableCopy {
    pub schema: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCopyState {
    pub sql: Arc<String>,
    pub rows: usize,
    pub bytes: usize,
    pub bytes_per_sec: usize,
    pub last_update: SystemTime,
}

impl Default for TableCopyState {
    fn default() -> Self {
        Self {
            sql: Arc::new(String::default()),
            rows: 0,
            bytes: 0,
            bytes_per_sec: 0,
            last_update: SystemTime::now(),
        }
    }
}

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
    pub password: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StatementKind {
    Table,
    Index,
    Statement,
}

impl std::fmt::Display for StatementKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Index => write!(f, "index"),
            Self::Statement => write!(f, "statement"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SyncState {
    PreData,
    PostData,
    Cutover,
    PostCutover,
}

impl std::fmt::Display for SyncState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreData => write!(f, "pre_data"),
            Self::PostData => write!(f, "post_data"),
            Self::Cutover => write!(f, "cutover"),
            Self::PostCutover => write!(f, "post_cutover"),
        }
    }
}

/// Database/user pair that identifies a database cluster pool.
#[derive(Debug, PartialEq, Hash, Eq, Clone, Default, Serialize, Deserialize)]
pub struct User {
    /// User name.
    pub user: String,
    /// Database name.
    pub database: String,
}
