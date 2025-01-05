//! Configuration.

pub mod error;

use error::Error;

use std::fs::read_to_string;
use std::sync::Arc;

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tracing::info;

static CONFIG: Lazy<ArcSwap<Config>> = Lazy::new(|| ArcSwap::from_pointee(Config::default()));

/// Load configuration.
pub fn config() -> Arc<Config> {
    CONFIG.load().clone()
}

/// Load the configuration file from disk.
pub fn load() -> Result<Config, Error> {
    if let Ok(config) = read_to_string("pgdog.toml") {
        info!("Loading pgdog.toml");
        Ok(toml::from_str(&config)?)
    } else {
        info!("Loading default configuration");
        Ok(Config::default())
    }
}

/// Configuration.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Config {
    /// General configuration.
    #[serde(default)]
    pub general: General,
    /// Statistics.
    #[serde(default)]
    pub stats: Stats,
    /// Databases and pools.
    #[serde(default = "Databases::default")]
    pub databases: Databases,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct General {
    #[serde(default = "General::host")]
    pub host: String,
    #[serde(default = "General::port")]
    pub port: u16,
    #[serde(default = "General::workers")]
    pub workers: usize,
}

impl General {
    fn host() -> String {
        "0.0.0.0".into()
    }

    fn port() -> u16 {
        6432
    }

    fn workers() -> usize {
        0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Stats {}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Databases {}
