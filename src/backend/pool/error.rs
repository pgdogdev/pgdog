//! Connection pool errors.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("checkout timeout")]
    CheckoutTimeout,

    #[error("server error")]
    ServerError,

    #[error("manual ban")]
    ManualBan,

    #[error("no replicas")]
    NoReplicas,
}
