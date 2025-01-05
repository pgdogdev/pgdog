//! pgDog backend managers connections to PostgreSQL.

pub mod error;
pub mod server;

pub use error::Error;
pub use server::Server;

pub mod databases;
pub mod pool;

pub use pool::{Cluster, Pool, Replicas, Shard};
