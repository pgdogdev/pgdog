use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("{0}")]
    Replication(#[from] crate::backend::replication::Error),

    #[error("{0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("{0}")]
    LogicalReplication(#[from] crate::backend::replication::logical::Error),

    #[error("pg_dump command failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("pg_dump error: {0}")]
    PgDump(String),

    #[error("{0}")]
    Syntax(#[from] pg_query::Error),

    #[error("parse error, stmt out of bounds")]
    StmtOutOfBounds,
}
