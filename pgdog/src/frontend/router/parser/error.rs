//! Parser error.

use thiserror::Error;

use crate::frontend::router::sharding;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    PgQuery(pg_query::Error),

    #[error("only CSV is supported for sharded copy")]
    OnlyCsv,

    #[error("no sharding column in CSV")]
    NoShardingColumn,

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("empty query")]
    EmptyQuery,

    #[error("not in sync")]
    NotInSync,

    #[error("no query in buffer")]
    NoQueryInBuffer,

    #[error("copy out of sync")]
    CopyOutOfSync,

    #[error("exceeded maximum number of rows in CSV parser")]
    MaxCsvParserRows,

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("binary copy signature incorrect")]
    BinaryMissingHeader,

    #[error("unexpected header extension")]
    BinaryHeaderExtension,

    #[error("set shard syntax error")]
    SetShard,

    #[error("no multi tenant id")]
    MultiTenantId,

    #[error("{0}")]
    Sharder(#[from] sharding::Error),

    #[error("missing parameter: ${0}")]
    MissingParameter(usize),

    #[error("shard selection is required but not provided")]
    ShardSelectionRequired,
}
