//! Manage connections to the servers.

pub mod address;
pub mod ban;
pub mod cleanup;
pub mod cluster;
pub mod comms;
pub mod config;
pub mod connection;
pub mod dns_cache;
pub mod error;
pub mod guard;
pub mod healthcheck;
pub mod inner;
pub mod mapping;
pub mod mirror_stats;
pub mod monitor;
pub mod oids;
pub mod pool_impl;
pub mod replicas;
pub mod request;
pub mod shard;
pub mod state;
pub mod stats;
pub mod taken;
pub mod waiting;

pub(crate) use address::Address;
pub(crate) use cluster::{Cluster, ClusterConfig, ClusterShardConfig, PoolConfig, ShardingSchema};
pub(crate) use config::Config;
pub(crate) use connection::Connection;
pub(crate) use error::Error;
pub(crate) use guard::Guard;
pub(crate) use healthcheck::Healtcheck;
pub(crate) use mirror_stats::MirrorStats;
use monitor::Monitor;
pub(crate) use oids::Oids;
pub(crate) use pool_impl::Pool;
pub(crate) use replicas::Replicas;
pub(crate) use request::Request;
pub(crate) use shard::Shard;
pub(crate) use state::State;
pub(crate) use stats::Stats;

use ban::Ban;
use comms::Comms;
use inner::Inner;
use mapping::Mapping;
use taken::Taken;
use waiting::{Waiter, Waiting};

#[cfg(test)]
pub mod test;
