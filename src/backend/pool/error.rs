//! Connection pool errors.
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("checkout timeout")]
    CheckoutTimeout,

    #[error("server error")]
    ServerError,

    #[error("manual ban")]
    ManualBan,

    #[error("no replicas")]
    NoReplicas,

    #[error("no such shard: {0}")]
    NoShard(usize),
}
