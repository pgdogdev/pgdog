//! Configuration errors.

use thiserror::Error;

/// Configuration error.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Deser(#[from] toml::de::Error),
}
