use std::path::PathBuf;

use tokio::process::Command;
use tracing::error;

use super::Error;

pub(super) struct Bins {
    pub(super) postgres: PathBuf,
    pub(super) initdb: PathBuf,
    pub(super) version: f32,
}

impl Bins {
    pub(super) async fn new() -> Result<Self, Error> {
        let pg_config = Command::new("pg_config").output().await?;

        if !pg_config.status.success() {
            error!(
                "[fdw] pg_config: {}",
                String::from_utf8_lossy(&pg_config.stderr)
            );
            return Err(Error::PgConfig);
        }

        let pg_config = String::from_utf8_lossy(&pg_config.stdout);
        let mut path = PathBuf::new();
        let mut version = 0.0;

        for line in pg_config.lines() {
            if line.starts_with("BINDIR") {
                let bin_dir = line.split("BINDIR = ").last().unwrap_or_default().trim();
                path = PathBuf::from(bin_dir);
            }
            if line.starts_with("VERSION") {
                version = line
                    .split("VERSION = ")
                    .last()
                    .unwrap_or_default()
                    .trim()
                    .split(" ")
                    .nth(1)
                    .unwrap_or_default()
                    .trim()
                    .parse()?;
            }
        }

        Ok(Self {
            postgres: path.join("postgres"),
            initdb: path.join("initdb"),
            version,
        })
    }
}
