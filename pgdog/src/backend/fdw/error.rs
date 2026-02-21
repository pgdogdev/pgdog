use std::num::ParseFloatError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("initdb failed")]
    InitDb,

    #[error("pg_config failed")]
    PgConfig,

    #[error("backend: {0}")]
    Backend(#[from] crate::backend::Error),

    #[error("pool: {0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("postgres didn't launch in time")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("nix: {0}")]
    Nix(#[from] nix::Error),

    #[error("shards don't have the same number of replicas/primary")]
    ShardsHostsMismatch,

    #[error("error parsing postgres version")]
    PostgresVersion(#[from] ParseFloatError),

    #[error("postgres process exited unexpectedly")]
    ProcessExited,

    #[error("fdw launcher channel closed")]
    ChannelClosed,
}
