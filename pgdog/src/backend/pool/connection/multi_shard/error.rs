//! Multi-shard specific errors.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("inconsistent row descriptions between shards: expected {expected} columns, got {actual} columns")]
    InconsistentRowDescription { expected: usize, actual: usize },

    #[error("inconsistent data types between shards: column {column_index} has type OID {expected} on some shards but {actual} on others")]
    InconsistentDataTypes {
        column_index: usize,
        expected: i32,
        actual: i32,
    },

    #[error("inconsistent column names between shards: column {column_index} has name '{expected}' on some shards but '{actual}' on others")]
    InconsistentColumnNames {
        column_index: usize,
        expected: String,
        actual: String,
    },

    #[error(
        "inconsistent column count in data rows: expected {expected} columns, got {actual} columns"
    )]
    InconsistentDataRowCount { expected: usize, actual: usize },

    #[error("net error: {0}")]
    Net(#[from] crate::net::Error),
}

impl From<crate::backend::Error> for Error {
    fn from(value: crate::backend::Error) -> Self {
        // Convert backend error to net error if it contains one, otherwise wrap as IO error
        match value {
            crate::backend::Error::Net(net_err) => Self::Net(net_err),
            other => Self::Net(crate::net::Error::Io(std::io::Error::other(format!(
                "{}",
                other
            )))),
        }
    }
}
