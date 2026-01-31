use std::path::{Path, PathBuf};
use tokio::{
    fs::{read_to_string, File},
    io::AsyncWriteExt,
};

use super::Error;

#[derive(Debug, Clone)]
pub(crate) struct ConfigParser {
    path: PathBuf,
    content: String,
}

impl ConfigParser {
    /// Load configuration from path.
    pub(crate) async fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = PathBuf::from(path.as_ref());
        // let content = read_to_string(&path).await?;

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
    pub(crate) async fn configure_and_save(&mut self, port: u16) -> Result<(), Error> {
        self.set("max_logical_replication_workers", "0");
        self.set("max_sync_workers_per_subscription", "0");
        self.set("max_parallel_apply_workers_per_subscription", "0");
        self.set("port", &port.to_string());
        self.set("max_connections", "100");
        self.set("log_line_prefix", "''");

        self.save().await?;

        println!("{}", read_to_string(&self.path).await.unwrap());

        Ok(())
    }
}
