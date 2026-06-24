//! Parser error.

use thiserror::Error;

use super::rewrite::statement::Error as RewriteError;
use crate::frontend::router::sharding;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    PgQuery(pg_query::Error),

    #[cfg(feature = "new_parser")]
    #[error("Error parsing query: {0}")]
    Parse(pg_raw_parse::Error),

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

    #[error("expected parameter ${0} to be an integer, got \'{1}\' instead")]
    ParameterNotInteger(usize, String),

    #[error("column has no associated table")]
    ColumnNoTable,

    #[error("query is blocked by plugin \"{0}\"")]
    BlockedByPlugin(String),

    #[error("this command requires a transaction")]
    RequiresTransaction,

    #[error("two-phase transaction control statements are not allowed when two-phase is enabled")]
    NoTwoPc,

    #[error("regex error")]
    RegexError,

    #[error("cross-shard truncate not supported when schema-sharding is used")]
    CrossShardTruncateSchemaSharding,

    #[error("prepared statement \"{0}\" doesn't exist")]
    PreparedStatementDoesntExist(String),

    #[error("column decode error")]
    ColumnDecode,

    #[error("table decode error")]
    TableDecode,

    #[error("parameter ${0} not in bind")]
    BindParameterMissing(i32),

    #[error("statement is not a SELECT")]
    NotASelect,

    #[error("rewrite: {0}")]
    Rewrite(#[from] RewriteError),

    #[error("sharded databases require the query parser to be enabled")]
    QueryParserRequired,

    #[error("multi-statement queries cannot mix SET with other commands")]
    MultiStatementMixedSet,
}
