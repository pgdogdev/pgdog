//! pgDog, modern PostgreSQL proxy, pooler and query router.

use backend::databases::databases;
use config::load;
use frontend::listener::Listener;
use tokio::runtime::Builder;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub mod admin;
pub mod auth;
pub mod backend;
pub mod channel;
pub mod config;
pub mod frontend;
pub mod net;
pub mod plugin;
pub mod state;
pub mod stats;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    info!("🐕 pgDog {}", env!("CARGO_PKG_VERSION"));

    let config = load()?;

    let runtime = match config.general.workers {
        0 => {
            let mut binding = Builder::new_current_thread();
            binding.enable_all();
            binding
        }
        workers => {
            info!("Spawning {} workers", workers);
            let mut builder = Builder::new_multi_thread();
            builder.worker_threads(workers).enable_all();
            builder
        }
    }
    .build()?;

    runtime.block_on(async move { pgdog().await })?;

    Ok(())
}

async fn pgdog() -> Result<(), Box<dyn std::error::Error>> {
    // Preload TLS.
    net::tls::load()?;

    // Load databases and connect if needed.
    databases();

    let mut listener = Listener::new("0.0.0.0:6432");
    listener.listen().await?;

    Ok(())
}
