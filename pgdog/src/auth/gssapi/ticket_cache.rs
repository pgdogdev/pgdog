//! Per-server Kerberos ticket cache

use super::error::{GssapiError, Result};
use parking_lot::RwLock;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[cfg(feature = "gssapi")]
use crate::auth::gssapi::ScopedEnv;

#[cfg(feature = "gssapi")]
use std::fs;

#[cfg(feature = "gssapi")]
use {
    libgssapi::{
        credential::{Cred, CredUsage},
        name::Name,
        oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
    },
    std::sync::Arc,
    tokio::task::spawn_blocking,
    uuid::Uuid,
};

/// Cache for a single server's Kerberos ticket
#[cfg(feature = "gssapi")]
pub struct TicketCache {
    /// The principal for this cache
    principal: String,
    /// Path to the keytab file
    keytab_path: PathBuf,
    /// Credential cache backing file
    cache_file: parking_lot::Mutex<Option<Arc<CacheFile>>>,
    /// Optional krb5 configuration path to set while acquiring
    krb5_config: Option<String>,
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
    pub fn new(
        principal: impl Into<String>,
        keytab_path: impl Into<PathBuf>,
        krb5_config: Option<String>,
    ) -> Self {
        Self {
            principal: principal.into(),
            keytab_path: keytab_path.into(),
            cache_file: parking_lot::Mutex::new(None),
            krb5_config,
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

        let cache_file = {
            let mut guard = self.cache_file.lock();
            if guard.is_none() {
                match CacheFile::new(&self.principal) {
                    Ok(file) => {
                        guard.replace(Arc::new(file));
                    }
                    Err(e) => {
                        return Err(GssapiError::CredentialAcquisitionFailed(format!(
                            "failed to prepare credential cache for {}: {}",
                            self.principal, e
                        )));
                    }
                }
            }
            guard.as_ref().unwrap().clone()
        };

        let principal = self.principal.clone();
        let keytab_path = self.keytab_path.clone();
        let cache_uri = cache_file.uri();

        let krb5_config = self.krb5_config.clone();

        let acquire = spawn_blocking(move || -> Result<Cred> {
            let mut overrides: Vec<(&'static str, Option<String>)> = vec![
                ("KRB5CCNAME", Some(cache_uri.clone())),
                (
                    "KRB5_CLIENT_KTNAME",
                    Some(keytab_path.to_string_lossy().into_owned()),
                ),
            ];

            if let Some(config) = &krb5_config {
                overrides.push(("KRB5_CONFIG", Some(config.clone())));
            }

            let guard = ScopedEnv::set(overrides);

            let name = Name::new(principal.as_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))
                .map_err(|e| GssapiError::InvalidPrincipal(format!("{}: {}", principal, e)))?;

            let mut desired_mechs = OidSet::new()
                .map_err(|e| GssapiError::LibGssapi(format!("failed to create OidSet: {}", e)))?;

            desired_mechs
                .add(&GSS_MECH_KRB5)
                .map_err(|e| GssapiError::LibGssapi(format!("failed to add mechanism: {}", e)))?;

            Cred::acquire(Some(&name), None, CredUsage::Initiate, Some(&desired_mechs))
                .map_err(|e| {
                    GssapiError::CredentialAcquisitionFailed(format!(
                        "credential acquisition failed for {}: {}",
                        principal, e
                    ))
                })
                .map(|cred| {
                    drop(guard);
                    cred
                })
        })
        .await
        .map_err(|_| {
            GssapiError::CredentialAcquisitionFailed(format!(
                "credential acquisition task panicked for {}",
                self.principal
            ))
        })?;

        match acquire {
            Ok(cred) => {
                let cred_arc: Arc<Cred> = Arc::new(cred);
                *self.credential.write() = Some(cred_arc.clone());
                *self.last_refresh.write() = Instant::now();
                Ok(cred_arc)
            }
            Err(err) => Err(err),
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
        self.ensure_cache_removed();
    }

    fn ensure_cache_removed(&self) {
        if let Some(cache) = self.cache_file.lock().take() {
            cache.cleanup();
        }
    }
}

#[cfg(feature = "gssapi")]
impl Drop for TicketCache {
    fn drop(&mut self) {
        self.ensure_cache_removed();
    }
}

#[cfg(feature = "gssapi")]
#[derive(Debug)]
struct CacheFile {
    path: PathBuf,
}

#[cfg(feature = "gssapi")]
impl CacheFile {
    fn new(principal: &str) -> std::io::Result<Self> {
        let sanitized: String = principal
            .chars()
            .map(|ch| match ch {
                'A'..='Z' | 'a'..='z' | '0'..='9' => ch,
                _ => '_',
            })
            .collect();

        let filename = format!("krb5cc_pgdog_{}_{}", sanitized, Uuid::new_v4());
        let path = std::env::temp_dir().join(filename);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let _ = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&path)?;
        }

        #[cfg(not(unix))]
        {
            let _ = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
        }

        Ok(Self { path })
    }

    fn uri(&self) -> String {
        format!("FILE:{}", self.path.display())
    }

    fn cleanup(&self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(feature = "gssapi")]
impl Drop for CacheFile {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(not(feature = "gssapi"))]
impl TicketCache {
    /// Create a new ticket cache
    pub fn new(
        principal: impl Into<String>,
        keytab_path: impl Into<PathBuf>,
        _krb5_config: Option<String>,
    ) -> Self {
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
        let cache = TicketCache::new("test@EXAMPLE.COM", "/etc/test.keytab", None);
        assert_eq!(cache.principal(), "test@EXAMPLE.COM");
        assert_eq!(cache.keytab_path(), &PathBuf::from("/etc/test.keytab"));
    }

    #[tokio::test]
    async fn test_missing_keytab_error() {
        let cache = TicketCache::new("test@REALM", "/nonexistent/keytab", None);
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
        let mut cache = TicketCache::new("test@REALM", "/etc/test.keytab", None);
        cache.set_refresh_interval(Duration::from_secs(3600));
        assert!(!cache.needs_refresh()); // Just created, doesn't need refresh
    }
}
