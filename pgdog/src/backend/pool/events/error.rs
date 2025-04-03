use thiserror::Error;

#[derive(Debug, Error, PartialEq, Clone, Copy)]
pub enum Error {
    #[error("watcher closed")]
    Watcher,
}

impl From<tokio::sync::watch::error::RecvError> for Error {
    fn from(_: tokio::sync::watch::error::RecvError) -> Self {
        Error::Watcher
    }
}
