//! pgDog, modern PostgreSQL proxy, pooler and query router.

use clap::Parser;
use pgdog::backend::databases;
use pgdog::cli::{self, Commands};
use pgdog::config;
use pgdog::frontend::listener::Listener;
use pgdog::net;
use pgdog::plugin;
use pgdog::stats;
use tokio::runtime::Builder;
use tracing::info;

use std::process::exit;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Cli::parse();

    pgdog::logger();

    let mut overrides = pgdog::config::Overrides::default();

    match args.command {
        Some(Commands::Fingerprint { query, path }) => {
            pgdog::cli::fingerprint(query, path)?;
            exit(0);
        }

        Some(Commands::Schema) => (),

        Some(Commands::Run {
            pool_size,
            min_pool_size,
            session_mode,
        }) => {
            overrides = pgdog::config::Overrides {
                min_pool_size,
                session_mode,
                default_pool_size: pool_size,
            };
        }

        None => (),
    }

    info!("🐕 PgDog v{}", env!("GIT_HASH"));

    let config = if let Some(database_urls) = args.database_url {
        config::from_urls(&database_urls)?
    } else {
        config::load(&args.config, &args.users)?
    };

    config::overrides(overrides);

    plugin::load_from_config()?;

    let runtime = match config.config.general.workers {
        0 => {
            let mut binding = Builder::new_current_thread();
            binding.enable_all();
            binding
        }
        workers => {
            info!("spawning {} workers", workers);
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
    // Preload TLS. Resulting primitives
    // are async, so doing this after Tokio launched seems prudent.
    net::tls::load()?;

    // Load databases and connect if needed.
    databases::init();

    let general = &config::config().config.general;

    if let Some(broadcast_addr) = general.broadcast_address {
        net::discovery::Listener::get().run(broadcast_addr, general.broadcast_port);
    }

    if let Some(openmetrics_port) = general.openmetrics_port {
        tokio::spawn(async move { stats::http_server::server(openmetrics_port).await });
    }

    let stats_logger = stats::StatsLogger::new();

    if general.dry_run {
        stats_logger.spawn();
    }

    let mut listener = Listener::new(format!("{}:{}", general.host, general.port));
    listener.listen().await?;

    info!("🐕 pgDog is shutting down");
    stats_logger.shutdown();

    // Any shutdown routines go below.
    plugin::shutdown();

    Ok(())
}
