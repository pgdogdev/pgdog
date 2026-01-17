use std::{array::TryFromSliceError, ffi::NulError};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    ParseInt(String),

    #[error("{0}")]
    Size(#[from] TryFromSliceError),

    #[error("{0}")]
    Uuid(#[from] uuid::Error),

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("context incomplete")]
    IncompleteContext,

    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("wrong integer binary size")]
    IntegerSize,

    #[error("{0}")]
    NullError(#[from] NulError),

    #[error("btree node error")]
    BtreeNodeError,

    #[error("range is overlapping or incorrect")]
    IncorrectRange,

    #[error("config has more than one sharding function")]
    MultipleShardingFunctions,

    #[error("sharding key value isn't valid")]
    InvalidValue,

    #[error("config error: {0}")]
    ConfigError(#[from] pgdog_config::Error),

    #[error("{0}")]
    TypeError(#[from] pgdog_postgres_types::Error),
}
