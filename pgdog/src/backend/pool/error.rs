//! Connection pool errors.
use thiserror::Error;

use crate::net::BackendKeyData;

#[derive(Debug, Error, PartialEq, Clone, Copy)]
pub enum Error {
    #[error("checkout timeout")]
    CheckoutTimeout,

    #[error("connect timeout")]
    ConnectTimeout,

    #[error("replica checkout timeout")]
    ReplicaCheckoutTimeout,

    #[error("server error")]
    ServerError,

    #[error("manual ban")]
    ManualBan,

    #[error("no replicas")]
    NoReplicas,

    #[error("no such shard: {0}")]
    NoShard(usize),

    #[error("pool is banned")]
    Banned,

    #[error("healthcheck timeout")]
    HealthcheckTimeout,

    #[error("healthcheck error")]
    HealthcheckError,

    #[error("primary lsn query failed")]
    PrimaryLsnQueryFailed,

    #[error("replica lsn query failed")]
    ReplicaLsnQueryFailed,

    #[error("pool is shut down")]
    Offline,

    #[error("no primary")]
    NoPrimary,

    #[error("no databases")]
    NoDatabases,

    #[error("config values contain null bytes")]
    NullBytes,

    #[error("all replicas down")]
    AllReplicasDown,

    #[error("router error")]
    Router,

    #[error("pub/sub disabled")]
    PubSubDisabled,

    #[error("pool {0} has no health target")]
    PoolNoHealthTarget(u64),

    #[error("pool is not healthy")]
    PoolUnhealthy,

    #[error("checked in untracked connection: {0}")]
    UntrackedConnCheckin(BackendKeyData),

    #[error("mapping missing: {0}")]
    MappingMissing(usize),

    #[error("fast shutdown failed")]
    FastShutdown,

    #[error("replica lag")]
    ReplicaLag,
}

impl Error {
    /// Transient availability fault worth retrying.
    ///
    /// Non-retryable: config errors, admin decisions, programming errors.
    /// Everything else (timeouts, server faults, lag, health misses) is transient.
    pub fn is_retryable(&self) -> bool {
        !matches!(
            self,
            // Config / wiring errors — retrying changes nothing.
            Self::NullBytes
                | Self::NoShard(_)
                | Self::NoDatabases
                | Self::PubSubDisabled
                | Self::PoolNoHealthTarget(_)
                | Self::MappingMissing(_)
                // Admin decisions — respect them.
                | Self::ManualBan
                // Programming errors.
                | Self::UntrackedConnCheckin(_)
                // Deliberate shutdown.
                | Self::FastShutdown
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable() {
        assert!(Error::CheckoutTimeout.is_retryable());
        assert!(Error::ConnectTimeout.is_retryable());
        assert!(Error::ReplicaCheckoutTimeout.is_retryable());
        assert!(Error::NoPrimary.is_retryable());
        assert!(Error::AllReplicasDown.is_retryable());
        assert!(Error::Banned.is_retryable());
        assert!(Error::NoReplicas.is_retryable());
        assert!(Error::ServerError.is_retryable());
        assert!(Error::HealthcheckTimeout.is_retryable());
        assert!(Error::HealthcheckError.is_retryable());
        assert!(Error::PrimaryLsnQueryFailed.is_retryable());
        assert!(Error::ReplicaLsnQueryFailed.is_retryable());
        assert!(Error::Offline.is_retryable());
        assert!(Error::ReplicaLag.is_retryable());
        assert!(Error::PoolUnhealthy.is_retryable());
    }

    #[test]
    fn not_retryable() {
        assert!(!Error::ManualBan.is_retryable());
        assert!(!Error::NullBytes.is_retryable());
        assert!(!Error::NoDatabases.is_retryable());
        assert!(!Error::PubSubDisabled.is_retryable());
        assert!(!Error::FastShutdown.is_retryable());
        assert!(!Error::NoShard(0).is_retryable());
        assert!(!Error::MappingMissing(0).is_retryable());
    }
}
