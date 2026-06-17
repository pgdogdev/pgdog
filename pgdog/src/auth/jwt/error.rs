/// Errors that can occur during JWT validation or key loading.
#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("Token expired")]
    TokenExpired,

    #[error("Invalid algorithm")]
    InvalidAlgorithm,

    #[error("Missing sub claim")]
    MissingSub,

    #[error("Audience mismatch")]
    AudienceMismatch,

    #[error("Missing kid")]
    MissingKid,

    #[error("JWK not found")]
    JwkNotFound,

    #[error("Public key load error: {0}")]
    PublicKeyLoad(String),

    #[error("JWKS fetch error: {0}")]
    JwksFetch(String),

    #[error("Invalid token: {0}")]
    InvalidToken(jsonwebtoken::errors::Error),

    #[error("Invalid JWK: {0}")]
    InvalidJwk(String),
}

impl From<jsonwebtoken::errors::Error> for JwtError {
    fn from(err: jsonwebtoken::errors::Error) -> Self {
        match err.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => JwtError::TokenExpired,
            jsonwebtoken::errors::ErrorKind::InvalidAlgorithm => JwtError::InvalidAlgorithm,
            jsonwebtoken::errors::ErrorKind::InvalidAudience => JwtError::AudienceMismatch,
            _ => JwtError::InvalidToken(err),
        }
    }
}
