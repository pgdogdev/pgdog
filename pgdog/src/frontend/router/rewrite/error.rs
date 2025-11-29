use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parser error")]
    ParserError,

    #[error("unique id: {0}")]
    UniqueId(#[from] crate::unique_id::Error),

    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("rewrite engine didn't rewrite bind")]
    NoBind,

    #[error("empty query")]
    EmptyQuery,

    #[error("no rewrite")]
    NoRewrite,

    #[error("prepared statement not found: {0}")]
    PreparedStatementNotFound(String),
}
