use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("[vault] HTTP error: {0}")]
    Http(String),

    #[error("[vault] unexpected status {status}: {body}")]
    VaultStatus { status: u16, body: String },

    #[error("[vault] response parse error: {0}")]
    Parse(String),

    #[error("[vault] secret_id not available: {0}")]
    SecretId(String),

    #[error("[vault] config update failed: {0}")]
    ConfigUpdate(String),
}
