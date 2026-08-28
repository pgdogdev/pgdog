use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Net(#[from] crate::net::Error),
}
