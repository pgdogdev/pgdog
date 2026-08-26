//! Administer the pooler.

use async_trait::async_trait;

use crate::net::messages::Message;

pub(crate) mod ban;
pub(crate) mod copy_data;
pub(crate) mod cutover;
pub(crate) mod error;
pub(crate) mod healthcheck;
pub(crate) mod maintenance_mode;
pub(crate) mod named_row;
pub(crate) mod parser;
pub(crate) mod pause;
pub(crate) mod prelude;
pub(crate) mod probe;
pub(crate) mod reconnect;
pub(crate) mod reload;
pub(crate) mod replicate;
pub(crate) mod reset_prepared;
pub(crate) mod reset_query_cache;
pub(crate) mod reshard;
pub(crate) mod schema_sync;
pub(crate) mod server;
pub(crate) mod set;
pub(crate) mod setup_schema;
pub(crate) mod show_bans;
pub(crate) mod show_client_memory;
pub(crate) mod show_clients;
pub(crate) mod show_config;
pub(crate) mod show_guc;
pub(crate) mod show_instance_id;
pub(crate) mod show_listeners;
pub(crate) mod show_lists;
pub(crate) mod show_mirrors;
pub(crate) mod show_peers;
pub(crate) mod show_pools;
pub(crate) mod show_prepared_statements;
pub(crate) mod show_query_cache;
pub(crate) mod show_replication;
pub(crate) mod show_replication_slots;
pub(crate) mod show_schema_sync;
pub(crate) mod show_server_memory;
pub(crate) mod show_servers;
pub(crate) mod show_stats;
pub(crate) mod show_table_copies;
pub(crate) mod show_tasks;
pub(crate) mod show_transactions;
pub(crate) mod show_version;
pub(crate) mod shutdown;
pub(crate) mod stop_task;

pub(crate) use ban::*;
pub(crate) use copy_data::*;
pub(crate) use cutover::*;
pub(crate) use error::Error;
pub(crate) use healthcheck::*;
pub(crate) use maintenance_mode::*;
pub(crate) use pause::*;
pub(crate) use probe::*;
pub(crate) use reconnect::*;
pub(crate) use reload::*;
pub(crate) use replicate::*;
pub(crate) use reset_prepared::*;
pub(crate) use reset_query_cache::*;
pub(crate) use reshard::*;
pub(crate) use schema_sync::*;
pub(crate) use set::*;
pub(crate) use setup_schema::*;
pub(crate) use show_bans::*;
pub(crate) use show_client_memory::*;
pub(crate) use show_clients::*;
pub(crate) use show_config::*;
pub(crate) use show_guc::*;
pub(crate) use show_instance_id::*;
pub(crate) use show_listeners::*;
pub(crate) use show_lists::*;
pub(crate) use show_mirrors::*;
pub(crate) use show_peers::*;
pub(crate) use show_pools::*;
pub(crate) use show_prepared_statements::*;
pub(crate) use show_query_cache::*;
pub(crate) use show_replication::*;
pub(crate) use show_replication_slots::*;
pub(crate) use show_schema_sync::*;
pub(crate) use show_server_memory::*;
pub(crate) use show_servers::*;
pub(crate) use show_stats::*;
pub(crate) use show_table_copies::*;
pub(crate) use show_tasks::*;
pub(crate) use show_transactions::*;
pub(crate) use show_version::*;
pub(crate) use shutdown::*;
pub(crate) use stop_task::*;

#[cfg(test)]
mod tests;

/// All pooler commands implement this trait.
#[async_trait]
pub(crate) trait Command: Sized {
    /// Execute the command and return results to the client.
    async fn execute(&self) -> Result<Vec<Message>, Error>;
    /// Command name.
    fn name(&self) -> String;
    /// Parse SQL and construct a command handler.
    fn parse(sql: &str) -> Result<Self, Error>;
}
