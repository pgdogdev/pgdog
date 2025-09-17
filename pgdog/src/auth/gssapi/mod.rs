//! GSSAPI authentication module for PGDog.
//!
//! This module provides Kerberos/GSSAPI authentication support for both
//! frontend (client to PGDog) and backend (PGDog to PostgreSQL) connections.

pub mod context;
pub mod error;
pub mod server;
pub mod ticket_cache;
pub mod ticket_manager;

#[cfg(test)]
mod tests;

pub use context::GssapiContext;
pub use error::{GssapiError, Result};
pub use server::GssapiServer;
pub use ticket_cache::TicketCache;
pub use ticket_manager::TicketManager;

use std::sync::Arc;
use tokio::sync::Mutex;

/// Handle GSSAPI authentication from a client
pub async fn handle_gssapi_auth(
    server: Arc<Mutex<GssapiServer>>,
    client_token: Vec<u8>,
) -> Result<GssapiResponse> {
    let mut server = server.lock().await;

    match server.accept(&client_token)? {
        Some(response_token) => {
            // More negotiation needed
            Ok(GssapiResponse {
                is_complete: false,
                token: Some(response_token),
                principal: None,
            })
        }
        None => {
            // Authentication complete
            let principal = server
                .client_principal()
                .ok_or_else(|| GssapiError::ContextError("No client principal found".to_string()))?
                .to_string();

            Ok(GssapiResponse {
                is_complete: true,
                token: None,
                principal: Some(principal),
            })
        }
    }
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
