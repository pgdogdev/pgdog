use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("prepared statement \"{0}\" does not exist")]
    MissingStatement(String),
}
