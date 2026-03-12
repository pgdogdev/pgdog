#![allow(clippy::len_without_is_empty)]
#![allow(clippy::result_unit_err)]
#![deny(clippy::print_stdout)]

pub mod admin;
pub mod auth;
pub mod backend;
pub mod cli;
pub mod config;
pub mod frontend;
pub mod healthcheck;
pub mod net;
pub mod plugin;
pub mod sighup;
pub mod state;
pub mod stats;
#[cfg(feature = "tui")]
pub mod tui;
pub mod unique_id;
pub mod util;

use pgdog_config::{General, LogFormat};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[cfg(test)]
use std::alloc::System;
use std::io::IsTerminal;

#[cfg(not(test))]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(test)]
#[global_allocator]
static GLOBAL: &stats_alloc::StatsAlloc<System> = &stats_alloc::INSTRUMENTED_SYSTEM;

/// Setup the logger, so `info!`, `debug!`
/// and other macros actually output something.
///
/// Using try_init and ignoring errors to allow
/// for use in tests (setting up multiple times).
pub fn logger() {
    init_logger(None);
}

/// Setup the logger using PgDog configuration.
pub fn logger_with_config(general: &General) {
    init_logger(Some(general));
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

    match general
        .map(|general| general.log_format)
        .unwrap_or_default()
    {
        LogFormat::Text => {
            let format = fmt::layer()
                .with_ansi(std::io::stderr().is_terminal())
                .with_writer(std::io::stderr)
                .with_file(false);
            #[cfg(not(debug_assertions))]
            let format = format.with_target(false);

            let _ = tracing_subscriber::registry()
                .with(format)
                .with(filter)
                .try_init();
        }
        LogFormat::Json => {
            let format = fmt::layer()
                .json()
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .with_file(false)
                .with_current_span(false)
                .with_span_list(false);
            #[cfg(not(debug_assertions))]
            let format = format.with_target(false);

            let _ = tracing_subscriber::registry()
                .with(format)
                .with(filter)
                .try_init();
        }
    }
}
