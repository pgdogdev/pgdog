use crate::net::Error as NetError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(
        "missing column at index {0} that was assumed to be present (this indicates a bug in pgdog, please open an issue with details about your query)"
    )]
    RequiredColumnMissing(usize),

    #[error("type: {0}")]
    Type(#[from] pgdog_postgres_types::Error),

    #[error("unsupported aggregation {function}: {reason}")]
    UnsupportedAggregation { function: String, reason: String },

    #[error("unexpected aggregation error: {0}")]
    Unexpected(String),
}

impl From<crate::net::Error> for Error {
    fn from(error: crate::net::Error) -> Self {
        match error {
            NetError::RequiredColumnMissing(index) => Self::RequiredColumnMissing(index),
            NetError::TypeError(error) => Self::Type(error),
            error => Self::Unexpected(error.to_string()),
        }
    }
}
