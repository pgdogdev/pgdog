//! Per-server Kerberos ticket cache

use super::error::{GssapiError, Result};
use parking_lot::RwLock;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::task::spawn_blocking;
use tokio::time::timeout;

#[cfg(feature = "gssapi")]
use std::sync::Arc;

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
    pub async fn acquire_ticket(&self) -> Result<Arc<Cred>> {
        // Check if keytab exists
        if !self.keytab_path.exists() {
            return Err(GssapiError::KeytabNotFound(self.keytab_path.clone()));
        }

        // Create a unique credential cache for this principal to avoid conflicts
        // This prevents "Principal in credential cache does not match desired name" errors
        // when the environment has a cache with a different principal
        let cache_file = format!(
            "/tmp/krb5cc_pgdog_{}_{}",
            self.principal.replace(['@', '.', '/'], "_"),
            std::process::id()
        );
        let cache_path = format!("FILE:{}", cache_file);

        // Clone values needed in the async task
        let principal = self.principal.clone();
        let keytab_path = self.keytab_path.clone();

        // Use kinit to acquire the ticket with proper timeout
        // This avoids the blocking issue with libgssapi's Cred::acquire
        let kinit_result = timeout(Duration::from_secs(5), async {
            let mut command = tokio::process::Command::new("kinit");
            command
                .arg("-kt")
                .arg(&keytab_path)
                .arg(&principal)
                .env("KRB5CCNAME", &cache_path)
                .kill_on_drop(true); // Important: kill the process if we drop the handle

            // Try to find and set KRB5_CONFIG if available
            for path in &["/opt/homebrew/etc/krb5.conf", "/etc/krb5.conf"] {
                if tokio::fs::metadata(path).await.is_ok() {
                    command.env("KRB5_CONFIG", path);
                    break;
                }
            }

            command.output().await
        })
        .await;

        let credential = match kinit_result {
            Ok(Ok(output)) if output.status.success() => {
                // kinit succeeded, now acquire the credential from the cache
                // Set up the environment
                std::env::set_var("KRB5CCNAME", &cache_path);
                std::env::set_var("KRB5_CLIENT_KTNAME", &keytab_path);

                // Use spawn_blocking but with the credential already in cache
                // This should be fast and not block
                timeout(
                    Duration::from_millis(500), // Much shorter timeout since creds should be in cache
                    spawn_blocking(move || -> std::result::Result<Cred, GssapiError> {
                        // Parse the principal name
                        let name = Name::new(principal.as_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))
                            .map_err(|e| {
                                GssapiError::InvalidPrincipal(format!("{}: {}", principal, e))
                            })?;

                        // Create the desired mechanisms set
                        let mut desired_mechs = OidSet::new().map_err(|e| {
                            GssapiError::LibGssapi(format!("failed to create OidSet: {}", e))
                        })?;
                        desired_mechs.add(&GSS_MECH_KRB5).map_err(|e| {
                            GssapiError::LibGssapi(format!("failed to add mechanism: {}", e))
                        })?;

                        // Acquire credentials from the cache that kinit populated
                        Cred::acquire(Some(&name), None, CredUsage::Initiate, Some(&desired_mechs))
                            .map_err(|e| {
                                GssapiError::CredentialAcquisitionFailed(format!(
                                    "failed to acquire from cache for {}: {}",
                                    principal, e
                                ))
                            })
                    }),
                )
                .await
            }
            Ok(Ok(output)) => {
                // kinit failed - return error immediately without trying libgssapi
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(GssapiError::CredentialAcquisitionFailed(format!(
                    "kinit failed for {}: {}",
                    self.principal, stderr
                )));
            }
            Ok(Err(e)) => {
                // Failed to run kinit
                return Err(GssapiError::CredentialAcquisitionFailed(format!(
                    "failed to run kinit for {}: {}",
                    self.principal, e
                )));
            }
            Err(_) => {
                // Timeout expired
                return Err(GssapiError::CredentialAcquisitionFailed(format!(
                    "kinit timed out after 5 seconds for {}",
                    self.principal
                )));
            }
        };

        match credential {
            Ok(Ok(Ok(cred))) => {
                // Successfully acquired credential from cache
                let cred_arc: Arc<Cred> = Arc::new(cred);

                // Store the credential
                *self.credential.write() = Some(cred_arc.clone());
                *self.last_refresh.write() = Instant::now();

                Ok(cred_arc)
            }
            Ok(Ok(Err(e))) => {
                // Failed to acquire from cache
                Err(e)
            }
            Ok(Err(_)) => {
                // spawn_blocking task panicked
                Err(GssapiError::CredentialAcquisitionFailed(format!(
                    "credential acquisition task panicked for {}",
                    self.principal
                )))
            }
            Err(_) => {
                // Timeout on credential acquisition from cache
                Err(GssapiError::CredentialAcquisitionFailed(format!(
                    "failed to acquire credentials from cache (timed out) for {}",
                    self.principal
                )))
            }
        }
    }

    /// Get the cached credential, acquiring it if necessary
    pub async fn get_credential(&self) -> Result<Arc<Cred>> {
        // Check if we have a cached credential
        if let Some(cred) = self.credential.read().as_ref() {
            // Check if it needs refresh
            if self.last_refresh.read().elapsed() < self.refresh_interval {
                return Ok(cred.clone());
            }
        }

        // Need to acquire or refresh
        self.acquire_ticket().await
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
    pub async fn refresh(&self) -> Result<()> {
        self.acquire_ticket().await?;
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
    pub async fn acquire_ticket(&self) -> Result<()> {
        Err(GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Get the cached credential (mock)
    pub async fn get_credential(&self) -> Result<()> {
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
    pub async fn refresh(&self) -> Result<()> {
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

    #[tokio::test]
    async fn test_missing_keytab_error() {
        let cache = TicketCache::new("test@REALM", "/nonexistent/keytab");
        #[cfg(feature = "gssapi")]
        {
            let result = cache.acquire_ticket().await;
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
            let result = cache.acquire_ticket().await;
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
