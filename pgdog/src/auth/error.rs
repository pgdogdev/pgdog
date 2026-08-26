use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("incorrect salt size")]
    IncorrectSaltSize(#[from] std::array::TryFromSliceError),

    #[error("server-side auth can only use one password")]
    ServerSideOnePassword,
}
