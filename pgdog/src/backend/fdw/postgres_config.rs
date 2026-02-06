use std::path::{Path, PathBuf};
use tokio::{fs::File, io::AsyncWriteExt};

use super::Error;

#[derive(Debug, Clone)]
pub(crate) struct PostgresConfig {
    path: PathBuf,
    content: String,
}

impl PostgresConfig {
    /// Load configuration from path.
    pub(crate) async fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = PathBuf::from(path.as_ref());

        Ok(Self {
            path,
            content: String::new(),
        })
    }

    /// Add a setting
    pub(crate) fn set(&mut self, name: &str, value: &str) {
        self.content.push_str(&format!("{} = {}\n", name, value));
    }

    /// Save configuration.
    pub(crate) async fn save(&self) -> Result<(), Error> {
        let mut file = File::create(&self.path).await?;
        file.write_all(self.content.as_bytes()).await?;
        Ok(())
    }

    /// Configure default settings we need off/on.
    pub(crate) async fn configure_and_save(
        &mut self,
        port: u16,
        version: f32,
    ) -> Result<(), Error> {
        // Make it accessible via psql for debugging.
        self.set("listen_addresses", "'0.0.0.0'");
        self.set("port", &port.to_string());

        // Disable logical replication workers.
        self.set("max_logical_replication_workers", "0");
        self.set("max_sync_workers_per_subscription", "0");
        self.set("max_parallel_apply_workers_per_subscription", "0");

        self.set("max_connections", "1000");
        self.set("log_line_prefix", "''");
        self.set("log_connections", "on");
        self.set("log_disconnections", "on");
        // self.set("log_statement", "off");
        // Disable autovacuum. This is safe, this database doesn't write anything locally.
        self.set("autovacuum", "off");
        // Make the background writer do nothing.
        self.set("bgwriter_lru_maxpages", "0");
        self.set("bgwriter_delay", "10s");

        if version >= 18.0 {
            // Disable async io workers.
            self.set("io_method", "sync");
        }

        self.save().await?;

        Ok(())
    }
}
