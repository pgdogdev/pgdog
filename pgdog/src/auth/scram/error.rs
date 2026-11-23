//! SCRAM errors.
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("out of order auth")]
    OutOfOrder,

    #[error("server accepted authentication before SCRAM verification completed")]
    Incomplete,

    #[error("invalid server first message")]
    InvalidServerFirst(#[from] scram::Error),
}
