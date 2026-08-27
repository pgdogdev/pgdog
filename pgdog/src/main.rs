#![allow(clippy::len_without_is_empty)]
#![allow(clippy::result_unit_err)]
#![deny(clippy::print_stdout)]
#![allow(clippy::enum_variant_names)]
#![warn(clippy::large_futures)]

//! pgDog, modern PostgreSQL proxy, pooler and query router.

#[cfg(test)]
use std::alloc::System;
use std::fs::read_to_string;
use std::io::IsTerminal;
use std::path::Path;
use std::process::exit;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use backend::databases;
use clap::Parser;
use cli::Commands;
use config::config;
use frontend::client::query_engine::two_pc::Manager;
use frontend::listener::Listener;
use frontend::prepared_statements;
use tokio::runtime::Builder;
use tracing::{error, info, warn};
use util::pgdog_version;

use arc_swap::ArcSwapOption;
use pgdog_config::{General, LogFormat};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use tracing_throttle::{Policy, SuppressionSummary, TracingRateLimitLayer};

#[macro_use]
extern crate derive_more;

mod admin;
mod api;
mod auth;
mod backend;
mod cli;
mod config;
mod frontend;
mod healthcheck;
mod net;
mod plugin;
mod sighup;
mod state;
mod stats;
pub(crate) mod sync;
mod tasks;
#[cfg(test)]
mod test_utils;
mod unique_id;
mod util;

#[cfg(test)]
mod tests;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    enable_jemalloc_background_thread();

    let args = cli::Cli::parse();
    let command = args.command.clone();
    let mut overrides = config::Overrides::default();

    match command.as_ref() {
        Some(Commands::Hash { password }) => {
            cli::hash_password(password);
            exit(0);
        }

        Some(Commands::Run {
            pool_size,
            min_pool_size,
            session_mode,
        }) => {
            overrides = config::Overrides {
                min_pool_size: *min_pool_size,
                session_mode: *session_mode,
                default_pool_size: *pool_size,
            };
        }

        _ => (),
    }

    bootstrap_logger(&args.config);

    let nofile = util::raise_nofile_limit();

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
    info!("open file descriptor limit is {}", nofile);

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

    let runtime = build_runtime(
        config.config.general.workers,
        config.config.memory.stack_size,
    )?;

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
    // Run atexit handlers on SIGTERM (e.g. llvm-cov profile flushing).
    #[cfg(unix)]
    install_sigterm_handler();

    // Preload TLS. Resulting primitives
    // are async, so doing this after Tokio launched seems prudent.
    net::tls::load()?;

    // Load databases and connect if needed.
    databases::init()?;

    let general = &config().config.general;

    install_log_throttle(general);

    if let Some(broadcast_addr) = general.broadcast_address {
        net::discovery::Listener::get().run(broadcast_addr, general.broadcast_port);
    }

    if let Some(openmetrics_port) = general.openmetrics_port {
        let openmetrics_host = general.openmetrics_host.clone();
        tasks::spawn("openmetrics server", async move {
            stats::http_server::server(&openmetrics_host, openmetrics_port).await
        });
    }

    if config::config().config.otel.endpoint.is_some() {
        tasks::spawn("otel publisher", stats::otel_exporter::run());
    }

    if let Some(healthcheck_port) = general.healthcheck_port {
        tasks::spawn("http healthcheck server", async move {
            healthcheck::server(healthcheck_port).await
        });
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

            if general.two_phase_commit {
                if let Some(ref path) = general.two_phase_commit_wal_dir {
                    let checkpoint_interval =
                        Duration::from_millis(general.two_phase_commit_wal_checkpoint_interval);
                    let fsync_interval =
                        Duration::from_millis(general.two_phase_commit_wal_fsync_interval);
                    Manager::get()
                        .enable_wal(
                            path,
                            Some(checkpoint_interval),
                            general.two_phase_commit_wal_segment_size as usize,
                            fsync_interval,
                        )
                        .await?;
                } else {
                    warn!("[2pc] wal disabled, 2pc will run without crash recovery")
                }
            }

            let mut listener = Listener::new(format!("{}:{}", general.host, general.port));
            listener.listen().await?;
        }

        Some(ref command) => {
            if let Commands::DataSync { .. } = command {
                info!("🔄 entering data sync mode");
                let result = cli::data_sync(command.clone()).await;
                // Wait for the 2PC monitor to drain any in-flight cleanup
                // before the process exits, even on error.
                Manager::get().shutdown().await;
                databases::shutdown();

                if let Err(err) = result {
                    error!("{}", err);
                    return Err(err);
                }
            }

            if let Commands::SchemaSync { .. } = command {
                info!("🔄 entering schema sync mode");
                let result = cli::schema_sync(command.clone()).await;

                // Wait for the 2PC monitor to drain any in-flight cleanup
                // before the process exits, even on error.
                Manager::get().shutdown().await;
                databases::shutdown();

                if let Err(err) = result {
                    error!("{}", err);
                    return Err(err);
                }
            }

            if let Commands::Setup { database } = command {
                info!("🔄 entering setup mode");
                let result = cli::setup(database).await;

                Manager::get().shutdown().await;
                databases::shutdown();

                result?;
            }

            if let Commands::ReplicateAndCutover { .. } = command {
                info!("🔄 entering test mode");
                let result = cli::replicate_and_cutover(command.clone()).await;

                Manager::get().shutdown().await;
                databases::shutdown();

                result?;
            }

            if let Commands::Route { .. } = command {
                let result = cli::route(command.clone()).await;

                Manager::get().shutdown().await;
                databases::shutdown();

                if let Err(err) = result {
                    error!("{}", err);
                    return Err(err);
                }
            }
        }
    }

    tasks::shutdown().await;

    // Any shutdown routines go below.
    plugin::shutdown();

    info!("🐕 PgDog is shutting down");

    Ok(())
}

/// Install a SIGTERM handler that exits the process via [`exit`], running
/// `atexit` handlers. Without it, SIGTERM terminates the process outright,
/// which skips the llvm-cov profile flush (no .profraw written) used by
/// integration test coverage. Behavior is otherwise unchanged: PgDog stops
/// immediately.
#[cfg(unix)]
fn install_sigterm_handler() {
    use tokio::signal::unix::{SignalKind, signal};

    if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
        tokio::spawn(async move {
            sigterm.recv().await;
            info!("🐕 PgDog is shutting down immediately [SIGTERM]");
            exit(0);
        });
    }
}

fn build_runtime(workers: usize, stack_size: usize) -> std::io::Result<tokio::runtime::Runtime> {
    match workers {
        0 => Builder::new_current_thread()
            .enable_all()
            .thread_stack_size(stack_size)
            .build(),
        workers => {
            let mut builder = Builder::new_multi_thread();
            builder.worker_threads(workers);

            if workers > 2 {
                info!("🚀 using alternative Tokio timer");
                builder.enable_alt_timer();
            }

            builder.enable_all().thread_stack_size(stack_size).build()
        }
    }
}

fn bootstrap_logger(config_path: &Path) {
    let general = read_to_string(config_path)
        .ok()
        .and_then(|config| toml::from_str::<config::Config>(&config).ok())
        .map(|config| config.general)
        .unwrap_or_default();

    logger_with_config(&general);
}

#[cfg(not(test))]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(test)]
#[global_allocator]
static GLOBAL: &stats_alloc::StatsAlloc<System> = &stats_alloc::INSTRUMENTED_SYSTEM;

/// Enable jemalloc's background purge threads so freed memory is returned to
/// the OS after allocation bursts.
#[cfg(all(not(test), not(target_env = "msvc")))]
fn enable_jemalloc_background_thread() {
    let _ = tikv_jemalloc_ctl::background_thread::write(true);
}

/// No-op fallback where jemalloc is not the active allocator.
#[cfg(any(test, target_env = "msvc"))]
fn enable_jemalloc_background_thread() {}

/// Filter that dynamically installs or removes an inner
/// [`TracingRateLimitLayer`] at runtime.
///
/// The tracing global dispatcher can only be set once, and
/// [`TracingRateLimitLayer`] must be constructed inside a tokio runtime
/// (its summary emitter uses `tokio::spawn`). The logger is bootstrapped
/// before the runtime exists, so we install this filter up front and let
/// the caller swap the real throttle in once the runtime is running.
#[derive(Default, Clone)]
struct DynamicThrottle {
    inner: Arc<ArcSwapOption<TracingRateLimitLayer>>,
}

impl DynamicThrottle {
    fn set(&self, layer: TracingRateLimitLayer) {
        self.inner.store(Some(Arc::new(layer)));
    }
}

impl<S> Filter<S> for DynamicThrottle
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn callsite_enabled(&self, _meta: &'static Metadata<'static>) -> Interest {
        // Always reevaluate per event: the inner filter can appear or
        // disappear between callsite cache and call time.
        Interest::sometimes()
    }

    fn enabled(&self, meta: &Metadata<'_>, cx: &Context<'_, S>) -> bool {
        match self.inner.load().as_ref() {
            Some(throttle) => Filter::<S>::enabled(throttle.as_ref(), meta, cx),
            None => true,
        }
    }

    fn event_enabled(&self, event: &Event<'_>, cx: &Context<'_, S>) -> bool {
        match self.inner.load().as_ref() {
            Some(throttle) => Filter::<S>::event_enabled(throttle.as_ref(), event, cx),
            None => true,
        }
    }
}

/// Target for suppression summary events — matches the tracing-throttle
/// crate's internal target so they remain exempt from further throttling.
const SUMMARY_TARGET: &str = "tracing_throttle::summary";

fn format_suppression_summary(summary: &SuppressionSummary) {
    let count = summary.count;
    let Some(meta) = summary.metadata.as_ref() else {
        tracing::warn!(target: SUMMARY_TARGET, "event [repeated {} times]", count);
        return;
    };
    match meta.level.as_str() {
        "ERROR" => {
            tracing::error!(target: SUMMARY_TARGET, "{} [repeated {} times]", meta.message, count)
        }
        "WARN" => {
            tracing::warn!(target: SUMMARY_TARGET, "{} [repeated {} times]", meta.message, count)
        }
        "INFO" => {
            tracing::info!(target: SUMMARY_TARGET, "{} [repeated {} times]", meta.message, count)
        }
        "DEBUG" => {
            tracing::debug!(target: SUMMARY_TARGET, "{} [repeated {} times]", meta.message, count)
        }
        _ => tracing::trace!(target: SUMMARY_TARGET, "{} [repeated {} times]", meta.message, count),
    }
}

static THROTTLE: OnceLock<DynamicThrottle> = OnceLock::new();

fn throttle_handle() -> &'static DynamicThrottle {
    THROTTLE.get_or_init(DynamicThrottle::default)
}

/// Setup the logger, so `info!`, `debug!`
/// and other macros actually output something.
///
/// Using try_init and ignoring errors to allow
/// for use in tests (setting up multiple times).
#[cfg(test)]
fn logger() {
    init_logger(None);
}

/// Setup the logger using PgDog configuration.
fn logger_with_config(general: &General) {
    init_logger(Some(general));
}

/// Install the log-throttle filter using the configured dedup window and
/// threshold. Must be called from within a tokio runtime — the throttle's
/// summary emitter task is spawned with `tokio::spawn`.
///
/// No-op when throttling is disabled (window or threshold set to 0) or
/// when the logger was not initialized.
fn install_log_throttle(general: &General) {
    if general.log_dedup_window == 0 || general.log_dedup_threshold == 0 {
        return;
    }
    let window = Duration::from_millis(general.log_dedup_window);
    let Ok(policy) = Policy::time_window(general.log_dedup_threshold as usize, window) else {
        return;
    };
    let layer = TracingRateLimitLayer::builder()
        .with_policy(policy)
        .with_summary_interval(window)
        .with_active_emission(true)
        .with_summary_formatter(Arc::new(format_suppression_summary))
        .build();
    if let Ok(layer) = layer {
        throttle_handle().set(layer);
    }
}

fn init_logger(general: Option<&General>) {
    let filter = match general {
        Some(general) => EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .parse_lossy(general.log_level.as_str()),
        None => EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy(),
    };

    let throttle = throttle_handle().clone();

    let log_format = general
        .map(|general| general.log_format)
        .unwrap_or_default();

    let format = fmt::layer()
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .with_file(false);
    #[cfg(not(debug_assertions))]
    let format = format.with_target(false);

    match log_format {
        LogFormat::Text => {
            let format = format.with_filter(throttle);
            let _ = tracing_subscriber::registry()
                .with(format)
                .with(filter)
                .try_init();
        }
        LogFormat::Json | LogFormat::JsonFlattened => {
            let format = format.json().with_current_span(false);
            let format = match log_format {
                LogFormat::JsonFlattened => format.flatten_event(true),
                _ => format,
            };
            let format = format.with_filter(throttle);

            let _ = tracing_subscriber::registry()
                .with(format)
                .with(filter)
                .try_init();
        }
    }
}
