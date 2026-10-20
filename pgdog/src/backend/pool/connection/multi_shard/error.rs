//! Multi-shard specific errors.

use thiserror::Error;

use crate::{backend::Error as BackendError, net::Error as NetError};

use super::super::aggregate::Error as AggregateError;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(
        "inconsistent row descriptions between shards: expected {expected} columns, got {actual} columns"
    )]
    InconsistentRowDescription { expected: usize, actual: usize },

    #[error(
        "inconsistent column names between shards: column {column_index} has name '{expected}' on some shards but '{actual}' on others"
    )]
    InconsistentColumnNames {
        column_index: usize,
        expected: String,
        actual: String,
    },

    #[error(
        "inconsistent column count in data rows: expected {expected} columns, got {actual} columns"
    )]
    InconsistentDataRowCount { expected: usize, actual: usize },

    #[error("unexpected message, expected {0} got {1}")]
    UnexpectedMessage(char, char),

    #[error("CommandComplete contains no row counts")]
    CommandCompleteNoRows,

    #[error("not utf-8")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("{0}")]
    Aggregate(#[from] AggregateError),

    #[error("unexpected multi-shard error: {0}")]
    Unexpected(String),
}

impl Error {
    /// Caller attempted an aggregate we don't support.
    pub(crate) fn is_unsupported(&self) -> bool {
        matches!(self, Self::Aggregate(..))
    }
}

impl From<NetError> for Error {
    fn from(value: NetError) -> Self {
        match value {
            NetError::UnexpectedMessage(expected, actual) => {
                Self::UnexpectedMessage(expected, actual)
            }
            NetError::CommandCompleteNoRows => Self::CommandCompleteNoRows,
            NetError::Utf8(error) => Self::Utf8(error),
            other => Self::Unexpected(other.to_string()),
        }
    }
}

impl From<BackendError> for Error {
    fn from(value: BackendError) -> Self {
        match value {
            BackendError::Aggregate(error) => Self::Aggregate(error),
            BackendError::Net(error) => error.into(),
            other => Self::Unexpected(other.to_string()),
        }
    }
}
