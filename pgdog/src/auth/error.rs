use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("incorrect salt size")]
    IncorrectSaltSize(#[from] std::array::TryFromSliceError),

    #[error("server-side auth can only use one password")]
    ServerSideOnePassword,

    #[error("backend: {0}")]
    Backend(Box<crate::backend::Error>),
}

impl From<crate::backend::Error> for Error {
    fn from(value: crate::backend::Error) -> Self {
        Self::Backend(Box::new(value))
    }
}
