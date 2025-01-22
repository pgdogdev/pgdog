//! Parser error.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    PgQuery(pg_query::Error),

    #[error("only CSV is suppoted for sharded copy")]
    OnlyCsv,

    #[error("no sharding column in CSV")]
    NoShardingColumn,

    #[error("{0}")]
    Csv(#[from] csv::Error),
}
