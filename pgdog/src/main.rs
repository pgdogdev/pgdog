//! pgDog, modern PostgreSQL proxy, pooler and query router.

use std::fs::read_to_string;
use std::path::Path;
use std::process::exit;

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
use serde::Deserialize;
use tokio::runtime::Builder;
use tracing::{error, info};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Cli::parse();
    let command = args.command.clone();
    let mut overrides = pgdog::config::Overrides::default();

    match command.as_ref() {
        Some(Commands::Fingerprint { query, path }) => {
            pgdog::cli::fingerprint(query.clone(), path.clone())?;
            exit(0);
        }

        Some(Commands::Run {
            pool_size,
            min_pool_size,
            session_mode,
        }) => {
            overrides = pgdog::config::Overrides {
                min_pool_size: *min_pool_size,
                session_mode: *session_mode,
                default_pool_size: *pool_size,
            };
        }

        _ => (),
    }

    bootstrap_logger(&args.config);

    let config = match config::load(&args.config, &args.users) {
        Ok(config) => config,
        Err(err) => {
            if matches!(command.as_ref(), Some(Commands::Configcheck)) {
                error!("{}", err);
                exit(1);
            }
            return Err(Box::new(err));
        }
    };

    if matches!(command.as_ref(), Some(Commands::Configcheck)) {
        info!("✅ config valid");
        exit(0);
    }

    info!("🐕 PgDog {}", pgdog_version());

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
            binding
                .enable_all()
                .thread_stack_size(config.config.memory.stack_size);
            binding
        }
        workers => {
            let mut builder = Builder::new_multi_thread();
            builder
                .worker_threads(workers)
                .enable_all()
                .thread_stack_size(config.config.memory.stack_size);
            builder
        }
    }
    .build()?;

    info!(
        "spawning {} threads (stack size: {}MiB)",
        config.config.general.workers,
        config.config.memory.stack_size / 1024 / 1024
    );
    info!(
        "using \"{}\" unique 64-bit ID generator",
        config.config.general.unique_id_function
    );

    runtime.block_on(async move { pgdog(args.command).await })?;

    Ok(())
}

async fn pgdog(command: Option<Commands>) -> Result<(), Box<dyn std::error::Error>> {
    // Preload TLS. Resulting primitives
    // are async, so doing this after Tokio launched seems prudent.
    net::tls::load()?;

    // Load databases and connect if needed.
    databases::init()?;

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
                    return Err(err);
                }
            }

            if let Commands::SchemaSync { .. } = command {
                info!("🔄 entering schema sync mode");
                if let Err(err) = cli::schema_sync(command.clone()).await {
                    error!("{}", err);
                    return Err(err);
                }
            }

            if let Commands::Setup { database } = command {
                info!("🔄 entering setup mode");
                cli::setup(database).await?;
            }

            if let Commands::ReplicateAndCutover { .. } = command {
                info!("🔄 entering test mode");
                cli::replicate_and_cutover(command.clone()).await?;
            }

            if let Commands::Route { .. } = command {
                if let Err(err) = cli::route(command.clone()).await {
                    error!("{}", err);
                    return Err(err);
                }
            }
        }
    }

    info!("🐕 PgDog is shutting down");
    stats_logger.shutdown();

    // Any shutdown routines go below.
    plugin::shutdown();

    Ok(())
}

#[derive(Debug, Default, Deserialize)]
struct BootstrapLoggingConfig {
    #[serde(default)]
    general: BootstrapLoggingGeneral,
}

#[derive(Debug, Default, Deserialize)]
struct BootstrapLoggingGeneral {
    log_format: Option<pgdog::config::LogFormat>,
    log_level: Option<String>,
}

fn bootstrap_logger(config_path: &Path) {
    let mut general = pgdog::config::General::default();

    if let Ok(config) = read_to_string(config_path) {
        if let Ok(bootstrap) = toml::from_str::<BootstrapLoggingConfig>(&config) {
            if let Some(log_format) = bootstrap.general.log_format {
                general.log_format = log_format;
            }

            if let Some(log_level) = bootstrap.general.log_level {
                general.log_level = log_level;
            }
        }
    }

    pgdog::logger_with_config(&general);
}
