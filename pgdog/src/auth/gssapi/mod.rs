//! GSSAPI authentication module for PGDog.
//!
//! This module provides Kerberos/GSSAPI authentication support for both
//! frontend (client to PGDog) and backend (PGDog to PostgreSQL) connections.

pub mod context;
pub mod error;
pub mod ticket_cache;
pub mod ticket_manager;

pub use context::GssapiContext;
pub use error::{GssapiError, Result};
pub use ticket_cache::TicketCache;
pub use ticket_manager::TicketManager;

/// Handle GSSAPI authentication from a client
pub fn handle_gssapi_auth(_client_token: Vec<u8>) -> Result<GssapiResponse> {
    // TODO: Implement
    unimplemented!("handle_gssapi_auth not yet implemented")
}

/// Response from GSSAPI authentication handling
pub struct GssapiResponse {
    /// Whether authentication is complete
    pub is_complete: bool,
    /// Token to send back to client (if any)
    pub token: Option<Vec<u8>>,
    /// Principal name extracted from context (if complete)
    pub principal: Option<String>,
}
