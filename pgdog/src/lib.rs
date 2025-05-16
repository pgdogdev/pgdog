pub mod admin;
pub mod auth;
pub mod backend;
pub mod cli;
pub mod config;
pub mod frontend;
pub mod net;
pub mod plugin;
pub mod sighup;
pub mod state;
pub mod stats;
#[cfg(feature = "tui")]
pub mod tui;
pub mod util;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use std::io::IsTerminal;

/// Setup the logger, so `info!`, `debug!`
/// and other macros actually output something.
///
/// Using try_init and ignoring errors to allow
/// for use in tests (setting up multiple times).
pub fn logger() {
    let format = fmt::layer()
        .with_ansi(std::io::stderr().is_terminal())
        .with_file(false);
    #[cfg(not(debug_assertions))]
    let format = format.with_target(false);

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let _ = tracing_subscriber::registry()
        .with(format)
        .with(filter)
        .try_init();
}
