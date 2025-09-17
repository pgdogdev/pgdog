//! GSSAPI-specific error types

use std::fmt;
use std::path::PathBuf;

/// Result type for GSSAPI operations
pub type Result<T> = std::result::Result<T, GssapiError>;

/// GSSAPI-specific errors
#[derive(Debug)]
pub enum GssapiError {
    /// Keytab file not found
    KeytabNotFound(PathBuf),

    /// Invalid principal name
    InvalidPrincipal(String),

    /// Ticket has expired
    TicketExpired,

    /// Failed to acquire credentials
    CredentialAcquisitionFailed(String),

    /// GSSAPI context error
    ContextError(String),

    /// Token processing error
    TokenError(String),

    /// Refresh failed
    RefreshFailed(String),

    /// Internal libgssapi error
    LibGssapi(String),

    /// I/O error
    Io(std::io::Error),

    /// Configuration error
    Config(String),
}

impl fmt::Display for GssapiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeytabNotFound(path) => write!(f, "Keytab file not found: {}", path.display()),
            Self::InvalidPrincipal(principal) => write!(f, "Invalid principal: {}", principal),
            Self::TicketExpired => write!(f, "Kerberos ticket has expired"),
            Self::CredentialAcquisitionFailed(msg) => {
                write!(f, "Failed to acquire credentials: {}", msg)
            }
            Self::ContextError(msg) => write!(f, "GSSAPI context error: {}", msg),
            Self::TokenError(msg) => write!(f, "Token processing error: {}", msg),
            Self::RefreshFailed(msg) => write!(f, "Ticket refresh failed: {}", msg),
            Self::LibGssapi(msg) => write!(f, "GSSAPI library error: {}", msg),
            Self::Io(err) => write!(f, "I/O error: {}", err),
            Self::Config(msg) => write!(f, "Configuration error: {}", msg),
        }
    }
}

impl std::error::Error for GssapiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for GssapiError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

// Convert libgssapi errors when we implement the actual functionality
#[cfg(feature = "gssapi")]
impl From<libgssapi::error::Error> for GssapiError {
    fn from(err: libgssapi::error::Error) -> Self {
        Self::LibGssapi(err.to_string())
    }
}
