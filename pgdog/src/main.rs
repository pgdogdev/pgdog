//! pgDog, modern PostgreSQL proxy, pooler and query router.

use clap::Parser;
use pgdog::backend::databases;
use pgdog::backend::pool::dns_cache::DnsCache;
use pgdog::cli::{self, Commands};
use pgdog::config::{self, config};
use pgdog::frontend::listener::Listener;
use pgdog::frontend::prepared_statements;
use pgdog::plugin;
use pgdog::stats;
use pgdog::util::pgdog_version;
use pgdog::{healthcheck, net};
use tokio::runtime::Builder;
use tracing::{error, info};

use std::process::exit;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Cli::parse();

    pgdog::logger();

    let mut overrides = pgdog::config::Overrides::default();

    match args.command {
        Some(Commands::Fingerprint { query, path }) => {
            pgdog::cli::fingerprint(query, path)?;
            exit(0);
        }

        Some(Commands::Configcheck) => {
            if let Err(e) = config::load(&args.config, &args.users) {
                error!("{}", e);
                exit(1);
            }

            info!("✅ config valid");
            exit(0);
        }

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

        _ => (),
    }

    info!("🐕 PgDog {}", pgdog_version());
    let config = config::load(&args.config, &args.users)?;

    // Get databases from environment or from --database-url args.
    let config = if let Some(database_urls) = args.database_url {
        config::from_urls(&database_urls)?
    } else if let Ok(config) = config::from_env() {
        info!(
            "loaded {} databases from environment",
            config.config.databases.len()
        );
        config
    } else {
        config
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

    runtime.block_on(async move { pgdog(args.command).await })?;

    Ok(())
}

async fn pgdog(command: Option<Commands>) -> Result<(), Box<dyn std::error::Error>> {
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

    if let Some(healthcheck_port) = general.healthcheck_port {
        tokio::spawn(async move { healthcheck::server(healthcheck_port).await });
    }

    let dns_cache_override_enabled = general.dns_ttl().is_some();
    if dns_cache_override_enabled {
        DnsCache::global().start_refresh_loop();
    }

    let stats_logger = stats::StatsLogger::new();
    prepared_statements::start_maintenance();

    if general.dry_run {
        stats_logger.spawn();
    }

    match command {
        None | Some(Commands::Run { .. }) => {
            if config().config.general.dry_run {
                info!("dry run mode enabled");
            }

            let mut listener = Listener::new(format!("{}:{}", general.host, general.port));
            listener.listen().await?;
        }

        Some(ref command) => {
            if let Commands::DataSync { .. } = command {
                info!("🔄 entering data sync mode");
                if let Err(err) = cli::data_sync(command.clone()).await {
                    error!("{}", err);
                }
            }

            if let Commands::SchemaSync { .. } = command {
                info!("🔄 entering schema sync mode");
                if let Err(err) = cli::schema_sync(command.clone()).await {
                    error!("{}", err);
                }
            }

            if let Commands::Setup { database } = command {
                info!("🔄 entering setup mode");
                cli::setup(database).await?;
            }
        }
    }

    info!("🐕 PgDog is shutting down");
    stats_logger.shutdown();

    // Any shutdown routines go below.
    plugin::shutdown();

    Ok(())
}
