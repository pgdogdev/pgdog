//! Manage connections to the servers.

pub mod address;
pub mod ban;
pub mod cleanup;
pub mod cluster;
pub mod comms;
pub mod config;
pub mod connection;
pub mod error;
pub mod guard;
pub mod healthcheck;
pub mod inner;
pub mod mapping;
pub mod monitor;
pub mod pool_impl;
pub mod replicas;
pub mod shard;
pub mod state;
pub mod waiting;

pub use address::Address;
pub use cluster::{Cluster, PoolConfig};
pub use config::Config;
pub use connection::Connection;
pub use error::Error;
pub use guard::Guard;
pub use healthcheck::Healtcheck;
use monitor::Monitor;
pub use pool_impl::Pool;
pub use replicas::Replicas;
pub use shard::Shard;
pub use state::State;

use ban::Ban;
use comms::Comms;
use inner::Inner;
use mapping::Mapping;
use waiting::Waiting;

#[cfg(test)]
mod test;
