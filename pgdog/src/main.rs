//! pgDog, modern PostgreSQL proxy, pooler and query router.

use backend::databases;
use clap::Parser;
use frontend::listener::Listener;
use tokio::runtime::Builder;
use tokio::select;
use tokio::signal::ctrl_c;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use std::io::IsTerminal;

pub mod admin;
pub mod auth;
pub mod backend;
pub mod channel;
pub mod cli;
pub mod comms;
pub mod config;
pub mod frontend;
pub mod net;
pub mod plugin;
pub mod state;
pub mod stats;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Cli::parse();

    let format = fmt::layer()
        .with_ansi(std::io::stderr().is_terminal())
        .with_file(false);

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::registry()
        .with(format)
        .with(filter)
        .init();

    info!("🐕 pgDog {}", env!("CARGO_PKG_VERSION"));

    let config = config::load(&args.config, &args.users)?;

    plugin::load_from_config()?;

    let runtime = match config.config.general.workers {
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
    let config = config::config();
    databases::from_config(&config);

    let mut listener = Listener::new("0.0.0.0:6432");
    listener.listen().await?;
    plugin::shutdown();

    Ok(())
}
