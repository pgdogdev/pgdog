use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("plugin error")]
    PluginError(#[from] std::ffi::NulError),

    #[error("{0}")]
    Net(#[from] crate::net::Error),

    #[error("{0}")]
    Backend(#[from] crate::backend::Error),

    #[error("{0}")]
    Pool(#[from] crate::backend::pool::Error),

    #[error("{0}")]
    Parser(#[from] super::parser::Error),
}
