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
}
