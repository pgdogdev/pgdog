use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("two phase transaction started already")]
    AlreadyStarted,

    #[error("{0}")]
    Backend(#[from] crate::backend::Error),
}
