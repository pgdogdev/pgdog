//! Administer the pooler.

use async_trait::async_trait;

use crate::net::messages::Message;

pub mod ban;
pub mod error;
pub mod healthcheck;
pub mod maintenance_mode;
pub mod named_row;
pub mod parser;
pub mod pause;
pub mod prelude;
pub mod probe;
pub mod reconnect;
pub mod reload;
pub mod reset_query_cache;
pub mod server;
pub mod set;
pub mod setup_schema;
pub mod show_client_memory;
pub mod show_clients;
pub mod show_config;
pub mod show_instance_id;
pub mod show_lists;
pub mod show_mirrors;
pub mod show_peers;
pub mod show_pools;
pub mod show_prepared_statements;
pub mod show_query_cache;
pub mod show_replication;
pub mod show_resharding_status;
pub mod show_server_memory;
pub mod show_servers;
pub mod show_stats;
pub mod show_transactions;
pub mod show_version;
pub mod shutdown;

pub use error::Error;

#[cfg(test)]
mod tests;

/// All pooler commands implement this trait.
#[async_trait]
pub trait Command: Sized {
    /// Execute the command and return results to the client.
    async fn execute(&self) -> Result<Vec<Message>, Error>;
    /// Command name.
    fn name(&self) -> String;
    /// Parse SQL and construct a command handler.
    fn parse(sql: &str) -> Result<Self, Error>;
}
