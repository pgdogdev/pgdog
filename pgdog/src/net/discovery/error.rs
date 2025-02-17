use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Encode(#[from] rmp_serde::encode::Error),

    #[error("{0}")]
    Decode(#[from] rmp_serde::decode::Error),

    #[error("{0}")]
    Io(#[from] tokio::io::Error),
}
