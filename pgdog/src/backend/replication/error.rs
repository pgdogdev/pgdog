use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("out of sync with unknown oid, expected Relation message first")]
    NoRelationMessage,

    #[error("no message to forward")]
    NoMessage,
}
