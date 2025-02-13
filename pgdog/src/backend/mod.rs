//! pgDog backend managers connections to PostgreSQL.

pub mod databases;
pub mod error;
pub mod pool;
pub mod replication;
pub mod schema;
pub mod server;
pub mod stats;

pub use error::Error;
pub use pool::{Cluster, Pool, Replicas, Shard};
pub use replication::ShardedTables;
pub use schema::Schema;
pub use server::Server;
pub use stats::Stats;
