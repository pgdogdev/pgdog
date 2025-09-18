//! Per-server Kerberos ticket cache

use super::error::{GssapiError, Result};
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "gssapi")]
use libgssapi::{
    credential::{Cred, CredUsage},
    name::Name,
    oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
};

/// Cache for a single server's Kerberos ticket
#[cfg(feature = "gssapi")]
pub struct TicketCache {
    /// The principal for this cache
    principal: String,
    /// Path to the keytab file
    keytab_path: PathBuf,
    /// The acquired credential (if any)
    credential: RwLock<Option<Arc<Cred>>>,
    /// When the ticket was last refreshed
    last_refresh: RwLock<Instant>,
    /// How often to refresh the ticket
    refresh_interval: Duration,
}

/// Mock ticket cache for when the feature is disabled
#[cfg(not(feature = "gssapi"))]
pub struct TicketCache {
    /// The principal for this cache
    principal: String,
    /// Path to the keytab file
    keytab_path: PathBuf,
    /// When the ticket was last refreshed
    last_refresh: RwLock<Instant>,
    /// How often to refresh the ticket
    refresh_interval: Duration,
}

#[cfg(feature = "gssapi")]
impl TicketCache {
    /// Create a new ticket cache
    pub fn new(principal: impl Into<String>, keytab_path: impl Into<PathBuf>) -> Self {
        Self {
            principal: principal.into(),
            keytab_path: keytab_path.into(),
            credential: RwLock::new(None),
            last_refresh: RwLock::new(Instant::now()),
            refresh_interval: Duration::from_secs(14400), // 4 hours default
        }
    }

    /// Set the refresh interval
    pub fn set_refresh_interval(&mut self, interval: Duration) {
        self.refresh_interval = interval;
    }

    /// Get the principal name
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Get the keytab path
    pub fn keytab_path(&self) -> &PathBuf {
        &self.keytab_path
    }

    /// Acquire a ticket from the keytab
    pub fn acquire_ticket(&self) -> Result<Arc<Cred>> {
        // Check if keytab exists
        if !self.keytab_path.exists() {
            return Err(GssapiError::KeytabNotFound(self.keytab_path.clone()));
        }

        // Set the KRB5_CLIENT_KTNAME environment variable to point to our keytab
        std::env::set_var("KRB5_CLIENT_KTNAME", &self.keytab_path);

        // Parse the principal name
        let name = Name::new(self.principal.as_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))
            .map_err(|e| GssapiError::InvalidPrincipal(format!("{}: {}", self.principal, e)))?;

        // Create the desired mechanisms set
        let mut desired_mechs = OidSet::new()
            .map_err(|e| GssapiError::LibGssapi(format!("failed to create OidSet: {}", e)))?;
        desired_mechs
            .add(&GSS_MECH_KRB5)
            .map_err(|e| GssapiError::LibGssapi(format!("failed to add mechanism: {}", e)))?;

        // Acquire credentials from the keytab
        let credential = Cred::acquire(
            Some(&name),
            None, // No specific time requirement
            CredUsage::Initiate,
            Some(&desired_mechs),
        )
        .map_err(|e| {
            GssapiError::CredentialAcquisitionFailed(format!(
                "failed for {}: {}",
                self.principal, e
            ))
        })?;

        let credential = Arc::new(credential);

        // Store the credential
        *self.credential.write() = Some(credential.clone());
        *self.last_refresh.write() = Instant::now();

        Ok(credential)
    }

    /// Get the cached credential, acquiring it if necessary
    pub fn get_credential(&self) -> Result<Arc<Cred>> {
        // Check if we have a cached credential
        if let Some(cred) = self.credential.read().as_ref() {
            // Check if it needs refresh
            if self.last_refresh.read().elapsed() < self.refresh_interval {
                return Ok(cred.clone());
            }
        }

        // Need to acquire or refresh
        self.acquire_ticket()
    }

    /// Check if the ticket needs refresh
    pub fn needs_refresh(&self) -> bool {
        self.last_refresh.read().elapsed() >= self.refresh_interval
    }

    /// Get the last refresh time
    pub fn last_refresh(&self) -> Instant {
        *self.last_refresh.read()
    }

    /// Refresh the ticket
    pub fn refresh(&self) -> Result<()> {
        self.acquire_ticket()?;
        Ok(())
    }

    /// Clear the cached credential
    pub fn clear(&self) {
        *self.credential.write() = None;
    }
}

#[cfg(not(feature = "gssapi"))]
impl TicketCache {
    /// Create a new ticket cache
    pub fn new(principal: impl Into<String>, keytab_path: impl Into<PathBuf>) -> Self {
        Self {
            principal: principal.into(),
            keytab_path: keytab_path.into(),
            last_refresh: RwLock::new(Instant::now()),
            refresh_interval: Duration::from_secs(14400), // 4 hours default
        }
    }

    /// Set the refresh interval
    pub fn set_refresh_interval(&mut self, interval: Duration) {
        self.refresh_interval = interval;
    }

    /// Get the principal name
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Get the keytab path
    pub fn keytab_path(&self) -> &PathBuf {
        &self.keytab_path
    }

    /// Acquire a ticket from the keytab (mock)
    pub fn acquire_ticket(&self) -> Result<()> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Get the cached credential (mock)
    pub fn get_credential(&self) -> Result<()> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Check if the ticket needs refresh
    pub fn needs_refresh(&self) -> bool {
        false
    }

    /// Get the last refresh time
    pub fn last_refresh(&self) -> Instant {
        *self.last_refresh.read()
    }

    /// Refresh the ticket (mock)
    pub fn refresh(&self) -> Result<()> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Clear the cached credential
    pub fn clear(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ticket_cache_creation() {
        let cache = TicketCache::new("test@EXAMPLE.COM", "/etc/test.keytab");
        assert_eq!(cache.principal(), "test@EXAMPLE.COM");
        assert_eq!(cache.keytab_path(), &PathBuf::from("/etc/test.keytab"));
    }

    #[test]
    fn test_missing_keytab_error() {
        let cache = TicketCache::new("test@REALM", "/nonexistent/keytab");
        #[cfg(feature = "gssapi")]
        {
            let result = cache.acquire_ticket();
            assert!(result.is_err());
            match result.unwrap_err() {
                GssapiError::KeytabNotFound(path) => {
                    assert_eq!(path, PathBuf::from("/nonexistent/keytab"));
                }
                _ => panic!("Expected KeytabNotFound error"),
            }
        }
        #[cfg(not(feature = "gssapi"))]
        {
            let result = cache.acquire_ticket();
            assert!(result.is_err());
            match result.unwrap_err() {
                GssapiError::LibGssapi(msg) => {
                    assert!(msg.contains("not compiled"));
                }
                _ => panic!("Expected LibGssapi error"),
            }
        }
    }

    #[test]
    fn test_refresh_interval() {
        let mut cache = TicketCache::new("test@REALM", "/etc/test.keytab");
        cache.set_refresh_interval(Duration::from_secs(3600));
        assert!(!cache.needs_refresh()); // Just created, doesn't need refresh
    }
}
