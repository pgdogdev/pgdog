//! pgDog backend managers connections to PostgreSQL.

pub(crate) mod auth;
pub(crate) mod connect_reason;
pub(crate) mod databases;
pub(crate) mod disconnect_reason;
pub(crate) mod error;
pub(crate) mod maintenance_mode;
pub(crate) mod pool;
pub(crate) mod prepared_statements;
pub(crate) mod protocol;
pub(crate) mod pub_sub;
pub(crate) mod reload_notify;
pub(crate) mod replication;
pub(crate) mod schema;
pub(crate) mod server;
pub(crate) mod server_options;
pub(crate) mod stats;
pub(crate) mod validation;

pub(crate) use connect_reason::ConnectReason;
pub(crate) use disconnect_reason::DisconnectReason;
pub(crate) use error::Error;
pub(crate) use pool::{
    CanonicalOids, Cluster, ClusterShardConfig, Oids, Pool, Shard, ShardingSchema,
};
pub(crate) use prepared_statements::PreparedStatements;
pub(crate) use protocol::*;
pub(crate) use pub_sub::{PubSubClient, PubSubListener};
pub(crate) use replication::ShardedTables;
pub(crate) use schema::Schema;
pub(crate) use server::Server;
pub(crate) use server_options::ServerOptions;
pub(crate) use stats::Stats;
