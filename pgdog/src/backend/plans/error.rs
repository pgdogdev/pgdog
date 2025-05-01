use thiserror::Error;

use crate::backend;

#[derive(Debug, Error)]
pub enum Error {
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("deser")]
    Deser,

    #[error("{0}")]
    Backend(#[from] backend::Error),

    #[error("server not in sync")]
    NotInSync,

    #[error("{0}")]
    Net(#[from] crate::net::Error),
}
