use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("incorrect salt size")]
    IncorrectSaltSize(#[from] std::array::TryFromSliceError),
}
