//! Server-side GSSAPI authentication handler.

use super::error::{GssapiError, Result};
use std::path::Path;
#[cfg(feature = "gssapi")]
use std::sync::Arc;

#[cfg(feature = "gssapi")]
use libgssapi::{
    context::{SecurityContext, ServerCtx},
    credential::{Cred, CredUsage},
    name::Name,
    oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
};

/// Server-side GSSAPI context for accepting client connections.
#[cfg(feature = "gssapi")]
pub struct GssapiServer {
    /// The underlying libgssapi server context.
    inner: Option<ServerCtx>,
    /// Server credentials.
    credential: Arc<Cred>,
    /// Whether the context establishment is complete.
    is_complete: bool,
    /// The authenticated client principal (once complete).
    client_principal: Option<String>,
}

/// Mock GSSAPI server for when the feature is disabled.
#[cfg(not(feature = "gssapi"))]
pub struct GssapiServer {
    /// Whether the context establishment is complete.
    is_complete: bool,
    /// The authenticated client principal (once complete).
    client_principal: Option<String>,
}

#[cfg(feature = "gssapi")]
impl GssapiServer {
    /// Create a new acceptor context.
    pub fn new_acceptor(keytab: impl AsRef<Path>, principal: Option<&str>) -> Result<Self> {
        let keytab = keytab.as_ref();

        // Set the keytab for the server
        std::env::set_var("KRB5_KTNAME", keytab);

        // Create credentials for accepting
        let credential = if let Some(principal) = principal {
            // Parse the service principal (use KRB5_PRINCIPAL to avoid canonicalization)
            let service_name = Name::new(principal.as_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))
                .map_err(|e| GssapiError::InvalidPrincipal(format!("{}: {}", principal, e)))?;

            // Create the desired mechanisms set
            let mut desired_mechs = OidSet::new()
                .map_err(|e| GssapiError::LibGssapi(format!("failed to create OidSet: {}", e)))?;
            desired_mechs
                .add(&GSS_MECH_KRB5)
                .map_err(|e| GssapiError::LibGssapi(format!("failed to add mechanism: {}", e)))?;

            // Acquire credentials for the specified principal
            Cred::acquire(
                Some(&service_name),
                None,
                CredUsage::Accept,
                Some(&desired_mechs),
            )
            .map_err(|e| {
                GssapiError::CredentialAcquisitionFailed(format!(
                    "failed to acquire credentials for {}: {}",
                    principal, e
                ))
            })?
        } else {
            // Use default service principal
            Cred::acquire(None, None, CredUsage::Accept, None).map_err(|e| {
                GssapiError::CredentialAcquisitionFailed(format!(
                    "failed to acquire default credentials: {}",
                    e
                ))
            })?
        };

        Ok(Self {
            inner: None,
            credential: Arc::new(credential),
            is_complete: false,
            client_principal: None,
        })
    }

    /// Process a token from the client.
    pub fn accept(&mut self, client_token: &[u8]) -> Result<Option<Vec<u8>>> {
        tracing::debug!(
            "GssapiServer::accept called with token of {} bytes",
            client_token.len()
        );
        tracing::trace!(
            "Token first 20 bytes: {:?}",
            &client_token[..client_token.len().min(20)]
        );

        if self.is_complete {
            tracing::warn!("GssapiServer::accept called but context already complete");
            return Err(GssapiError::ContextError(
                "context already complete".to_string(),
            ));
        }

        // Create or reuse the server context
        let mut ctx = match self.inner.take() {
            Some(ctx) => {
                tracing::debug!("reusing existing server context");
                ctx
            }
            None => {
                tracing::debug!("creating new server context");
                ServerCtx::new(Some(self.credential.as_ref().clone()))
            }
        };

        // Process the client token
        tracing::debug!("calling ctx.step with client token");
        match ctx.step(client_token) {
            Ok(Some(response)) => {
                // More negotiation needed
                tracing::debug!(
                    "ctx.step returned response token of {} bytes - negotiation continues",
                    response.len()
                );
                tracing::trace!(
                    "Response token first 20 bytes: {:?}",
                    &response[..response.len().min(20)]
                );

                // Check if context is actually established despite returning a token
                if ctx.is_complete() {
                    tracing::warn!("context is complete but still returned a token - this might confuse the client");
                    // Mark as complete and extract the principal
                    self.is_complete = true;
                    match ctx.source_name() {
                        Ok(name) => {
                            let principal = name.to_string();
                            tracing::debug!(
                                "Extracted client principal (with token): {}",
                                principal
                            );
                            self.client_principal = Some(principal);
                        }
                        Err(e) => {
                            tracing::error!("failed to get client principal: {}", e);
                        }
                    }
                }

                self.inner = Some(ctx);
                Ok(Some(response.to_vec()))
            }
            Ok(None) => {
                // Context established successfully
                tracing::debug!("ctx.step returned None - GSSAPI context established successfully");
                self.is_complete = true;

                // Extract the client principal
                match ctx.source_name() {
                    Ok(name) => {
                        let principal = name.to_string();
                        tracing::debug!("extracted client principal: {}", principal);
                        self.client_principal = Some(principal);
                    }
                    Err(e) => {
                        return Err(GssapiError::ContextError(format!(
                            "failed to get client principal: {}",
                            e
                        )));
                    }
                }

                self.inner = Some(ctx);
                tracing::debug!("GssapiServer::accept returning None (success)");
                Ok(None)
            }
            Err(e) => Err(GssapiError::ContextError(format!(
                "GSSAPI negotiation failed: {}",
                e
            ))),
        }
    }

    /// Check if the context establishment is complete.
    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    /// Get the authenticated client principal.
    pub fn client_principal(&self) -> Option<&str> {
        self.client_principal.as_deref()
    }
}

#[cfg(not(feature = "gssapi"))]
impl GssapiServer {
    /// Create a new acceptor context (mock version).
    pub fn new_acceptor(_keytab: impl AsRef<Path>, _principal: Option<&str>) -> Result<Self> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Process a token from the client (mock version).
    pub fn accept(&mut self, _client_token: &[u8]) -> Result<Option<Vec<u8>>> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Check if the context establishment is complete.
    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    /// Get the authenticated client principal.
    pub fn client_principal(&self) -> Option<&str> {
        self.client_principal.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_creation() {
        // This will fail without a real keytab
        let result = GssapiServer::new_acceptor(
            "/etc/pgdog/pgdog.keytab",
            Some("postgres/pgdog.example.com@EXAMPLE.COM"),
        );

        // We expect this to fail without a real keytab or when feature is disabled
        assert!(result.is_err());
    }
}
