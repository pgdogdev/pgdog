use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("requires enterprise edition")]
    EERequired,
}
