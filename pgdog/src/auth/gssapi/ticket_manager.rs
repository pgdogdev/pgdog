//! Global ticket manager for all backend servers

use super::error::Result;
use super::ticket_cache::TicketCache;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

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
}

impl TicketManager {
    /// Create a new ticket manager
    pub fn new() -> Self {
        Self {
            caches: Arc::new(DashMap::new()),
            refresh_tasks: Arc::new(DashMap::new()),
        }
    }

    /// Get the global ticket manager instance
    pub fn global() -> Arc<TicketManager> {
        INSTANCE.clone()
    }

    /// Get or acquire a ticket for a server
    /// Returns Ok(()) when the credential cache is ready to use
    #[cfg(feature = "gssapi")]
    pub fn get_ticket(
        &self,
        server: impl Into<String>,
        keytab: impl AsRef<Path>,
        principal: impl Into<String>,
    ) -> Result<()> {
        let server = server.into();
        let keytab_path = keytab.as_ref().to_path_buf();
        let principal = principal.into();

        // Check if we already have a cache for this server
        if self.caches.contains_key(&server) {
            // Cache already exists and environment is set
            return Ok(());
        }

        // Create a unique credential cache file for this server connection
        let cache_file = format!("/tmp/krb5cc_pgdog_{}", server.replace(":", "_"));
        let cache_path = format!("FILE:{}", cache_file);

        // Set the environment variable for this thread
        std::env::set_var("KRB5CCNAME", &cache_path);

        // Use kinit to get a ticket from the keytab into the unique cache
        let output = std::process::Command::new("kinit")
            .arg("-kt")
            .arg(&keytab_path)
            .arg(&principal)
            .env("KRB5CCNAME", &cache_path)
            .env("KRB5_CONFIG", "/opt/homebrew/etc/krb5.conf")
            .output()
            .map_err(|e| {
                super::error::GssapiError::LibGssapi(format!("Failed to run kinit: {}", e))
            })?;

        if !output.status.success() {
            return Err(super::error::GssapiError::CredentialAcquisitionFailed(
                format!(
                    "kinit failed for {}: {}",
                    principal,
                    String::from_utf8_lossy(&output.stderr)
                ),
            ));
        }

        // Create a new cache object for tracking (but don't acquire credentials - kinit already did that)
        let cache = Arc::new(TicketCache::new(principal, keytab_path));

        // Store the cache
        self.caches.insert(server.clone(), cache.clone());

        // Start background refresh task
        self.start_refresh_task(server, cache);

        // Return success - the credential cache is now populated and KRB5CCNAME is set
        Ok(())
    }

    /// Get or acquire a ticket for a server (mock version)
    #[cfg(not(feature = "gssapi"))]
    pub fn get_ticket(
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
        let _refresh_tasks = self.refresh_tasks.clone();
        let server_clone = server.clone();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Check every hour
            interval.tick().await; // Skip the immediate tick

            loop {
                interval.tick().await;

                if cache.needs_refresh() {
                    match cache.refresh() {
                        Ok(()) => {
                            tracing::info!("Refreshed ticket for {}", server_clone);
                        }
                        Err(e) => {
                            tracing::error!("Failed to refresh ticket for {}: {}", server_clone, e);
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

    #[test]
    fn test_cache_management() {
        let manager = TicketManager::new();

        // This will fail because the keytab doesn't exist, but it tests the structure
        let result = manager.get_ticket("server1:5432", "/nonexistent/keytab", "test@REALM");
        assert!(result.is_err());

        // Even though ticket acquisition failed, the cache should not be stored
        assert_eq!(manager.cache_count(), 0);
    }

    #[test]
    fn test_shutdown() {
        let manager = TicketManager::new();
        manager.shutdown();
        assert_eq!(manager.cache_count(), 0);
    }
}
