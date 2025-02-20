use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("prepared statement \"{0}\" is missing from cache")]
    MissingPreparedStatement(String),

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("wrong message")]
    WrongMessage,
}
