//! Manage connections to the servers.

pub(crate) mod address;
pub(crate) mod cleanup;
pub(crate) mod cluster;
pub(crate) mod cluster_metrics;
pub(crate) mod comms;
pub(crate) mod connection;
pub(crate) mod dns_cache;
pub(crate) mod ee;
pub(crate) mod error;
pub(crate) mod failover_signal;
pub(crate) mod guard;
pub(crate) mod healthcheck;
pub(crate) mod inner;
pub(crate) mod lb;
pub(crate) mod lsn_monitor;
pub(crate) mod monitor;
pub(crate) mod password;
pub(crate) mod pool_impl;
pub(crate) mod request;
pub(crate) mod role;
pub(crate) mod shard;
pub(crate) mod state;
pub(crate) mod stats;
pub(crate) mod taken;
pub(crate) mod token_cache;
pub(crate) mod waiting;

pub(crate) use address::Address;
pub(crate) use cluster::{Cluster, ClusterConfig, ClusterShardConfig, PoolConfig, ShardingSchema};
pub(crate) use cluster_metrics::ClusterMetrics;
pub(crate) use connection::Connection;
pub(crate) use error::Error;
pub(super) use failover_signal::ClusterFailoverSignalWatcher;
pub(crate) use guard::Guard;
pub(crate) use healthcheck::Healtcheck;
pub(crate) use lb::LoadBalancer;
pub(crate) use lsn_monitor::LsnStats;
pub(crate) use monitor::Monitor;
pub(crate) use password::Password;
pub(crate) use pool_impl::Pool;
pub(crate) use request::Request;
pub(crate) use role::PoolRole;
pub(crate) use shard::{CanonicalOids, Oids, Shard};
pub(crate) use state::State;
pub(crate) use stats::Stats;

pub use pgdog_config::pool::PoolConfig as Config;

use comms::Comms;
use inner::Inner;
use shard::ShardConfig;
use taken::Taken;
use waiting::{Waiter, Waiting};

#[cfg(test)]
pub(crate) mod test;
