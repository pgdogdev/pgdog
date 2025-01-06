use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("routing plugin missing")]
    RoutingPluginMissing,

    #[error("plugin error")]
    PluginError(#[from] std::ffi::NulError),

    #[error("no query in buffer")]
    NoQueryInBuffer,
}
