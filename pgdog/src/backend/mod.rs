//! pgDog backend managers connections to PostgreSQL.

pub mod databases;
pub mod error;
pub mod maintenance_mode;
pub mod pool;
pub mod prepared_statements;
pub mod protocol;
pub mod pub_sub;
pub mod reload_notify;
pub mod replication;
pub mod schema;
pub mod server;
pub mod server_options;
pub mod stats;

pub(crate) use error::Error;
pub(crate) use pool::{Cluster, ClusterShardConfig, Pool, ShardingSchema};
pub(crate) use prepared_statements::PreparedStatements;
pub(crate) use protocol::*;
pub(crate) use pub_sub::{PubSubClient, PubSubListener};
pub(crate) use replication::ShardedTables;
pub(crate) use schema::Schema;
pub(crate) use server::Server;
pub(crate) use server_options::ServerOptions;
pub(crate) use stats::Stats;
