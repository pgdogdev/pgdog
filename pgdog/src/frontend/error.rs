//! Frontend errors.

use std::io::ErrorKind;

use thiserror::Error;

use crate::unique_id;

/// Frontend error.
#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("{0}")]
    Router(#[from] super::router::Error),

    #[error("unexpected message: {0}")]
    UnexpectedMessage(char),

    #[error("scram error")]
    Scram(#[from] scram::Error),

    #[error("replication")]
    Replication(#[from] crate::backend::replication::Error),

    #[error("{0}")]
    PreparedStatements(#[from] super::prepared_statements::Error),

    #[error("query timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("cluster start timeout")]
    ClusterStart,

    #[error("join error")]
    Join(#[from] tokio::task::JoinError),

    #[error("unique id: {0}")]
    UniqueId(#[from] unique_id::Error),

    #[error("parser: {0}")]
    Parser(#[from] crate::frontend::router::parser::Error),

    #[error("rewrite: {0}")]
    Rewrite(#[from] crate::frontend::router::parser::rewrite::statement::Error),

    #[error("query has no route")]
    NoRoute,

    #[error("multi-tuple insert requires multi-shard binding")]
    MultiShardRequired,

    // FIXME: layer errors better so we don't have
    // to reach so deep into a module.
    #[error("{0}")]
    Multi(#[from] Box<crate::frontend::client::query_engine::multi_step::error::Error>),
}

impl From<crate::frontend::client::query_engine::multi_step::error::Error> for Error {
    fn from(value: crate::frontend::client::query_engine::multi_step::error::Error) -> Self {
        Self::Multi(Box::new(value))
    }
}

impl Error {
    pub(crate) fn disconnect(&self) -> bool {
        if let Error::Net(crate::net::Error::Io(err)) = self
            && err.kind() == ErrorKind::UnexpectedEof
        {
            return true;
        }

        if let Error::Net(crate::net::Error::UnexpectedEof) = self {
            return true;
        }

        false
    }
}
