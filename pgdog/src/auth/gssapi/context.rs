//! GSSAPI context wrapper for authentication negotiation

use super::error::{GssapiError, Result};
use std::path::Path;

#[cfg(feature = "gssapi")]
use libgssapi::{
    context::{ClientCtx, CtxFlags, SecurityContext},
    credential::{Cred, CredUsage},
    name::Name,
    oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
};

/// Wrapper for GSSAPI security context
#[cfg(feature = "gssapi")]
pub struct GssapiContext {
    /// The underlying libgssapi context
    inner: ClientCtx,
    /// The target service principal
    target_principal: String,
    /// Whether the context is complete
    is_complete: bool,
}

/// Mock GSSAPI context for when the feature is disabled.
#[cfg(not(feature = "gssapi"))]
pub struct GssapiContext {
    /// The target service principal
    target_principal: String,
    /// Whether the context is complete
    is_complete: bool,
}

#[cfg(feature = "gssapi")]
impl GssapiContext {
    /// Create a new initiator context (for connecting to a backend)
    pub fn new_initiator(
        keytab: impl AsRef<Path>,
        principal: impl Into<String>,
        target: impl Into<String>,
    ) -> Result<Self> {
        let _keytab = keytab.as_ref();
        let principal = principal.into();
        let target_principal = target.into();

        // TicketManager has already set up the credential cache with KRB5CCNAME
        // We just need to acquire credentials from that cache

        // Create the desired mechanisms set
        let mut desired_mechs = OidSet::new()
            .map_err(|e| GssapiError::LibGssapi(format!("Failed to create OidSet: {}", e)))?;
        desired_mechs
            .add(&GSS_MECH_KRB5)
            .map_err(|e| GssapiError::LibGssapi(format!("Failed to add mechanism: {}", e)))?;

        // Acquire credentials from the cache that TicketManager populated
        // Pass None to use the default principal from the cache
        let credential = Cred::acquire(
            None, // Use the principal from the cache that TicketManager set up
            None,
            CredUsage::Initiate,
            Some(&desired_mechs),
        )
        .map_err(|e| {
            GssapiError::CredentialAcquisitionFailed(format!("Failed for {}: {}", principal, e))
        })?;

        // Parse target service principal (use KRB5_PRINCIPAL to avoid hostname canonicalization)
        let target_name = Name::new(target_principal.as_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))
            .map_err(|e| GssapiError::InvalidPrincipal(format!("{}: {}", target_principal, e)))?;

        // Create the client context
        let flags = CtxFlags::GSS_C_MUTUAL_FLAG | CtxFlags::GSS_C_SEQUENCE_FLAG;

        let inner = ClientCtx::new(Some(credential), target_name, flags, Some(&GSS_MECH_KRB5));

        Ok(Self {
            inner,
            target_principal,
            is_complete: false,
        })
    }

    /// Initiate the GSSAPI handshake (get the first token)
    pub fn initiate(&mut self) -> Result<Vec<u8>> {
        match self.inner.step(None, None)? {
            Some(token) => Ok(token.to_vec()),
            None => {
                self.is_complete = true;
                Ok(Vec::new())
            }
        }
    }

    /// Process a response token from the server
    pub fn process_response(&mut self, token: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.inner.step(Some(token), None)? {
            Some(response) => Ok(Some(response.to_vec())),
            None => {
                self.is_complete = true;
                Ok(None)
            }
        }
    }

    /// Check if the context establishment is complete
    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    /// Get the target principal
    pub fn target_principal(&self) -> &str {
        &self.target_principal
    }

    /// Get the authenticated client name (for server contexts)
    pub fn client_name(&mut self) -> Result<String> {
        self.inner
            .source_name()
            .map(|name| name.to_string())
            .map_err(|e| GssapiError::ContextError(format!("Failed to get client name: {}", e)))
    }
}

#[cfg(not(feature = "gssapi"))]
impl GssapiContext {
    /// Create a new initiator context (mock version)
    pub fn new_initiator(
        _keytab: impl AsRef<Path>,
        _principal: impl Into<String>,
        target: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            target_principal: target.into(),
            is_complete: false,
        })
    }

    /// Initiate the GSSAPI handshake (mock version)
    pub fn initiate(&mut self) -> Result<Vec<u8>> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Process a response token from the server (mock version)
    pub fn process_response(&mut self, _token: &[u8]) -> Result<Option<Vec<u8>>> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Check if the context establishment is complete
    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    /// Get the target principal
    pub fn target_principal(&self) -> &str {
        &self.target_principal
    }

    /// Get the authenticated client name (mock version)
    pub fn client_name(&mut self) -> Result<String> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        // This will fail without a real keytab, but tests the API
        let result = GssapiContext::new_initiator(
            "/etc/test.keytab",
            "pgdog@EXAMPLE.COM",
            "postgres/db.example.com@EXAMPLE.COM",
        );

        #[cfg(feature = "gssapi")]
        assert!(result.is_err());

        #[cfg(not(feature = "gssapi"))]
        assert!(result.is_ok());
    }
}
