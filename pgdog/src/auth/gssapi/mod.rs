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
    tracing::debug!(
        "handle_gssapi_auth called with token of {} bytes",
        client_token.len()
    );
    let mut server = server.lock().await;

    tracing::debug!("calling server.accept()");
    match server.accept(&client_token)? {
        Some(response_token) => {
            // Check if authentication is complete despite having a token
            if server.is_complete() {
                let principal = server
                    .client_principal()
                    .ok_or_else(|| {
                        GssapiError::ContextError("no client principal found".to_string())
                    })?
                    .to_string();

                tracing::info!(
                    "Authentication complete (with final token), principal: {}",
                    principal
                );
                let response = GssapiResponse {
                    is_complete: true,
                    token: Some(response_token), // Send final token to client
                    principal: Some(principal.clone()),
                };
                tracing::debug!(
                    "Returning GssapiResponse: is_complete=true, has_token=true, principal={}",
                    principal
                );
                Ok(response)
            } else {
                // More negotiation needed
                tracing::info!(
                    "server.accept returned token of {} bytes - negotiation continues",
                    response_token.len()
                );
                let response = GssapiResponse {
                    is_complete: false,
                    token: Some(response_token),
                    principal: None,
                };
                tracing::debug!(
                    "Returning GssapiResponse: is_complete=false, has_token=true, principal=None"
                );
                Ok(response)
            }
        }
        None => {
            // Authentication complete
            tracing::info!("server.accept returned None - authentication complete");
            let principal = server
                .client_principal()
                .ok_or_else(|| GssapiError::ContextError("No client principal found".to_string()))?
                .to_string();

            tracing::info!("successfully extracted principal: {}", principal);
            let response = GssapiResponse {
                is_complete: true,
                token: None,
                principal: Some(principal.clone()),
            };
            tracing::debug!(
                "Returning GssapiResponse: is_complete=true, has_token=false, principal={}",
                principal
            );
            Ok(response)
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
