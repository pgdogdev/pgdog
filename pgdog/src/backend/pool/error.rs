//! Connection pool errors.
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Copy, Clone)]
pub enum Error {
    #[error("checkout timeout")]
    CheckoutTimeout,

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

    #[error("healtcheck timeout")]
    HealtcheckTimeout,

    #[error("healtcheck error")]
    HealtcheckError,

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
}
