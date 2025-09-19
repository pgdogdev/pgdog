//! Global ticket manager for all backend servers

use super::error::Result;
use super::ticket_cache::TicketCache;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

#[cfg(feature = "gssapi")]
use super::credential_provider::PrincipalLocks;

lazy_static! {
    /// Global ticket manager instance
    static ref INSTANCE: Arc<TicketManager> = Arc::new(TicketManager::new());
}

/// Manages Kerberos tickets for multiple backend servers
pub struct TicketManager {
    /// Map of server address to ticket cache
    caches: Arc<DashMap<String, Arc<TicketCache>>>,
    /// Background refresh tasks
    refresh_tasks: Arc<DashMap<String, JoinHandle<()>>>,
    #[cfg(feature = "gssapi")]
    // Per-principal acquisition locks
    principal_locks: PrincipalLocks,
}

impl TicketManager {
    /// Create a new ticket manager
    pub fn new() -> Self {
        Self {
            caches: Arc::new(DashMap::new()),
            refresh_tasks: Arc::new(DashMap::new()),
            #[cfg(feature = "gssapi")]
            principal_locks: PrincipalLocks::new(),
        }
    }

    /// Get the global ticket manager instance
    pub fn global() -> Arc<TicketManager> {
        INSTANCE.clone()
    }

    /// Get the appropriate krb5.conf path for the current system
    #[cfg(feature = "gssapi")]
    fn get_krb5_config() -> Option<String> {
        // First check if KRB5_CONFIG environment variable is set
        if let Ok(config) = std::env::var("KRB5_CONFIG") {
            return Some(config);
        }

        // Check common locations
        let paths = vec![
            "/etc/krb5.conf",              // Linux standard location
            "/opt/homebrew/etc/krb5.conf", // macOS Homebrew location
            "/usr/local/etc/krb5.conf",    // Alternative location
        ];

        for path in paths {
            if std::path::Path::new(path).exists() {
                return Some(path.to_string());
            }
        }

        None
    }

    /// Get or acquire a ticket for a server
    /// Returns Ok(()) when the credential cache is ready to use
    #[cfg(feature = "gssapi")]
    pub async fn get_ticket(
        &self,
        server: impl Into<String>,
        keytab: impl AsRef<Path>,
        principal: impl Into<String>,
    ) -> Result<()> {
        let server = server.into();
        let keytab_path = keytab.as_ref().to_path_buf();
        let principal = principal.into();

        if self.caches.contains_key(&server) {
            return Ok(());
        }

        let caches = Arc::clone(&self.caches);
        let manager = self;
        let server_clone = server.clone();
        let principal_clone = principal.clone();
        let krb5_config = Self::get_krb5_config();

        self.principal_locks
            .with_principal(&principal, || async move {
                if caches.contains_key(&server_clone) {
                    return Ok(());
                }

                let cache = Arc::new(TicketCache::new(
                    principal_clone.clone(),
                    keytab_path.clone(),
                    krb5_config.clone(),
                ));

                cache.get_credential().await.map(|_| ())?;

                caches.insert(server_clone.clone(), cache.clone());
                manager.start_refresh_task(server_clone, cache);
                Ok(())
            })
            .await
    }

    /// Get or acquire a ticket for a server (mock version)
    #[cfg(not(feature = "gssapi"))]
    pub async fn get_ticket(
        &self,
        _server: impl Into<String>,
        _keytab: impl AsRef<Path>,
        _principal: impl Into<String>,
    ) -> Result<()> {
        Err(super::error::GssapiError::LibGssapi(
            "GSSAPI support not compiled in".to_string(),
        ))
    }

    /// Start a background refresh task for a cache
    #[allow(dead_code)]
    fn start_refresh_task(&self, server: String, cache: Arc<TicketCache>) {
        let server_clone = server.clone();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Check every hour
            interval.tick().await; // Skip the immediate tick

            loop {
                interval.tick().await;

                if cache.needs_refresh() {
                    match cache.refresh().await {
                        Ok(()) => {
                            tracing::info!("[gssapi] refreshed ticket for \"{}\"", server_clone);
                        }
                        Err(e) => {
                            tracing::error!("failed to refresh ticket for {}: {}", server_clone, e);
                            // Continue trying - the old ticket might still be valid
                        }
                    }
                }
            }
        });

        self.refresh_tasks.insert(server, task);
    }

    /// Get a cache for a server (if it exists)
    pub fn get_cache(&self, server: &str) -> Option<Arc<TicketCache>> {
        self.caches.get(server).map(|entry| entry.clone())
    }

    /// Get the last refresh time for a server
    pub fn get_last_refresh(&self, server: &str) -> Option<Instant> {
        self.caches.get(server).map(|cache| cache.last_refresh())
    }

    /// Set the refresh interval for all future caches
    pub fn set_refresh_interval(&self, _interval: Duration) {
        // This would need to be stored and applied to new caches
        // For simplicity, we'll make this a no-op for now
    }

    /// Get the number of cached tickets
    pub fn cache_count(&self) -> usize {
        self.caches.len()
    }

    /// Shutdown the ticket manager, cleaning up all resources
    pub fn shutdown(&self) {
        // Cancel all refresh tasks
        for task in self.refresh_tasks.iter() {
            task.value().abort();
        }
        self.refresh_tasks.clear();

        // Clear all caches
        for cache in self.caches.iter() {
            cache.value().clear();
        }
        self.caches.clear();
    }

    /// Remove a specific server's cache
    pub fn remove_cache(&self, server: &str) {
        // Cancel the refresh task if it exists
        if let Some((_, task)) = self.refresh_tasks.remove(server) {
            task.abort();
        }

        // Remove the cache
        if let Some((_, cache)) = self.caches.remove(server) {
            cache.clear();
        }
    }
}

impl Default for TicketManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TicketManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ticket_manager_creation() {
        let manager = TicketManager::new();
        assert_eq!(manager.cache_count(), 0);
    }

    #[test]
    fn test_ticket_manager_global() {
        let manager1 = TicketManager::global();
        let manager2 = TicketManager::global();
        assert!(Arc::ptr_eq(&manager1, &manager2));
    }

    #[tokio::test]
    async fn test_cache_management() {
        let manager = TicketManager::new();

        // This will fail because the keytab doesn't exist, but it tests the structure
        let result = manager
            .get_ticket("server1:5432", "/nonexistent/keytab", "test@REALM")
            .await;
        assert!(result.is_err());

        // Even though ticket acquisition failed, the cache should not be stored
        assert_eq!(manager.cache_count(), 0);
    }

    #[cfg(feature = "gssapi")]
    #[tokio::test]
    async fn test_get_ticket_restores_env_on_error() {
        struct EnvGuard {
            key: &'static str,
            original: Option<String>,
        }

        impl EnvGuard {
            fn new(key: &'static str, replacement: &str) -> Self {
                let original = std::env::var(key).ok();
                std::env::set_var(key, replacement);
                Self { key, original }
            }
        }

        impl Drop for EnvGuard {
            fn drop(&mut self) {
                match self.original.as_ref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }

        let guard = EnvGuard::new("KRB5CCNAME", "FILE:pgdog-test-original");

        let manager = TicketManager::new();
        let result = manager
            .get_ticket(
                "testhost:5432",
                "/definitely/missing.keytab",
                "missing@REALM",
            )
            .await;
        assert!(result.is_err());

        let current = std::env::var("KRB5CCNAME").expect("env var should exist");
        assert_eq!(current, "FILE:pgdog-test-original");

        drop(guard);
    }

    #[test]
    fn test_shutdown() {
        let manager = TicketManager::new();
        manager.shutdown();
        assert_eq!(manager.cache_count(), 0);
    }
}
