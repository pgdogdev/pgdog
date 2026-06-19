use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("incorrect salt size")]
    IncorrectSaltSize(#[from] std::array::TryFromSliceError),

    #[error("server-side auth can only use one password")]
    ServerSideOnePassword,

    #[error("JWT error: {0}")]
    Jwt(#[from] super::jwt::JwtError),
}
