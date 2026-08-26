//! Parser error.

use thiserror::Error;

use super::rewrite::statement::Error as RewriteError;
use crate::frontend::router::sharding;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error parsing query: {0}")]
    Parse(#[from] pg_raw_parse::Error),

    #[error("no sharding column in CSV")]
    NoShardingColumn,

    #[error("cannot translate a binary sharding key of this data type through a lookup")]
    LookupBinaryCopy,

    #[error("sharding key lookup failed: {0}")]
    Lookup(String),

    #[error("cannot write to an omnisharded table with a shard directive")]
    OmniWriteWithDirective,

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("empty query")]
    EmptyQuery,

    #[error("exceeded maximum number of rows in CSV parser")]
    MaxCsvParserRows,

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("binary copy signature incorrect")]
    BinaryMissingHeader,

    #[error("unexpected header extension")]
    BinaryHeaderExtension,

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

    #[error("two-phase transaction control statements are not allowed when two-phase is enabled")]
    NoTwoPc,

    #[error("regex error")]
    RegexError,

    #[error("cross-shard truncate not supported when schema-sharding is used")]
    CrossShardTruncateSchemaSharding,

    #[error("column decode error")]
    ColumnDecode,

    #[error("table decode error")]
    TableDecode,

    #[error("rewrite: {0}")]
    Rewrite(#[from] RewriteError),

    #[error("sharded databases require the query parser to be enabled")]
    QueryParserRequired,

    #[error("multi-statement queries cannot mix SET with other commands")]
    MultiStatementMixedSet,

    #[error("unmapped sharding key was specified")]
    UnmappedShardKey(String),

    #[error("execute requires prepared statements to be set to full")]
    ExecuteRequiresFull,
}
