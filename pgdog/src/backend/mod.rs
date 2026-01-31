//! pgDog backend managers connections to PostgreSQL.

pub mod connect_reason;
pub mod databases;
pub mod disconnect_reason;
pub mod error;
pub mod fdw;
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

pub use connect_reason::ConnectReason;
pub use disconnect_reason::DisconnectReason;
pub use error::Error;
pub use pool::{Cluster, ClusterShardConfig, LoadBalancer, Pool, Shard, ShardingSchema};
pub use prepared_statements::PreparedStatements;
pub use protocol::*;
pub use pub_sub::{PubSubClient, PubSubListener};
pub use replication::ShardedTables;
pub use schema::Schema;
pub use server::Server;
pub use server_options::ServerOptions;
pub use stats::Stats;
