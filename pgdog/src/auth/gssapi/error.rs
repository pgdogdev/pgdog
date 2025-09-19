//! GSSAPI-specific error types

use std::path::PathBuf;
use thiserror::Error;

/// Result type for GSSAPI operations
pub type Result<T> = std::result::Result<T, GssapiError>;

/// GSSAPI-specific errors
#[derive(Debug, Error)]
pub enum GssapiError {
    /// Keytab file not found
    #[error("keytab file not found: {0}")]
    KeytabNotFound(PathBuf),

    /// Invalid principal name
    #[error("invalid principal: {0}")]
    InvalidPrincipal(String),

    /// Ticket has expired
    #[error("kerberos ticket has expired")]
    TicketExpired,

    /// Failed to acquire credentials
    #[error("failed to acquire credentials: {0}")]
    CredentialAcquisitionFailed(String),

    /// GSSAPI context error
    #[error("GSSAPI context error: {0}")]
    ContextError(String),

    /// Token processing error
    #[error("token processing error: {0}")]
    TokenError(String),

    /// Refresh failed
    #[error("ticket refresh failed: {0}")]
    RefreshFailed(String),

    /// Internal libgssapi error
    #[error("GSSAPI library error: {0}")]
    LibGssapi(String),

    /// I/O error
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),
}

// Convert libgssapi errors when we implement the actual functionality
#[cfg(feature = "gssapi")]
impl From<libgssapi::error::Error> for GssapiError {
    fn from(err: libgssapi::error::Error) -> Self {
        Self::LibGssapi(err.to_string())
    }
}
