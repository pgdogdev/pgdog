//! pgDog backend managers connections to PostgreSQL.

pub mod databases;
pub mod error;
pub mod pool;
pub mod prepared_statements;
pub mod protocol;
pub mod replication;
pub mod schema;
pub mod server;
pub mod stats;

pub use error::Error;
pub use pool::{Cluster, ClusterShardConfig, Pool, Replicas, Shard, ShardingSchema};
pub use prepared_statements::PreparedStatements;
pub use protocol::*;
pub use replication::ShardedTables;
pub use schema::Schema;
pub use server::Server;
pub use stats::Stats;
