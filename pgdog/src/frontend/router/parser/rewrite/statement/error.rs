use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unique_id generation failed: {0}")]
    UniqueId(#[from] crate::unique_id::Error),

    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("cache: {0}")]
    Cache(String),

    #[error("sharding key assignment unsupported: {0}")]
    UnsupportedShardingKeyUpdate(String),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("missing parameter: ${0}")]
    MissingParameter(u16),

    #[error("empty query")]
    EmptyQuery,

    #[error("missing column: ${0}")]
    MissingColumn(usize),

    #[error("WHERE clause is required")]
    WhereClauseMissing,

    #[error("{0}")]
    Type(#[from] pgdog_postgres_types::Error),

    #[error("primary key is missing")]
    MissingPrimaryKey,

    #[error("missing AST on request")]
    MissingAst,
}
