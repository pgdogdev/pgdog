use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("initdb failed")]
    InitDb,

    #[error("backend: {0}")]
    Backend(#[from] crate::backend::Error),

    #[error("postgres didn't launch in time")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("nix: {0}")]
    Nix(#[from] nix::Error),
}
