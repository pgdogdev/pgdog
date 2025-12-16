use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("backend: {0}")]
    Backend(#[from] crate::backend::Error),

    #[error("router: {0}")]
    Router(#[from] crate::frontend::router::Error),

    #[error("net: {0}")]
    Net(#[from] crate::net::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("multi-step insert has no route")]
    NoRoute,
}
