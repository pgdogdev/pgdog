use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum SchemaSyncError {
    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("{0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("pg_dump command failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("pg_dump error: {0}")]
    PgDump(String),

    #[error("{0}")]
    Syntax(#[from] pg_raw_parse::Error),

    #[error("parse error, stmt out of bounds")]
    StmtOutOfBounds,

    #[error("cluster has no databases")]
    NoDatabases,

    #[error("missing entity in dump")]
    MissingEntity,

    #[error("publication \"{0}\" has no tables")]
    PublicationNoTables(String),

    #[error("schema sync aborted")]
    Aborted,
}
