use thiserror::Error;

use crate::unique_id;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unique_id generation failed: {0}")]
    UniqueId(#[from] unique_id::Error),

    #[error("pg_query: {0}")]
    PgQuery(#[from] pg_query::Error),

    #[error("cache: {0}")]
    Cache(String),
}
