//! SCRAM errors.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("out of order auth")]
    OutOfOrder,

    #[error("invalid server first message")]
    InvalidServerFirst(#[from] scram::Error),

    #[error("auth failed")]
    AuthenticationFailed,
}
