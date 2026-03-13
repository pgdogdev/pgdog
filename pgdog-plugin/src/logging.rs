//! Logger initialization for plugins.
//!
//! Called automatically by the `#[config]` macro before the plugin's
//! config function runs, so plugins share PgDog's log level and format.

use std::io::IsTerminal;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::PdConfig;

pub fn init(config: &PdConfig) {
    let log_level = std::ops::Deref::deref(&config.log_level);
    let log_json = config.log_json == 1;

    let filter = EnvFilter::builder()
        .with_default_directive(log_level.parse().unwrap_or(LevelFilter::INFO.into()))
        .from_env_lossy();

    if log_json {
        let format = fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .with_file(false);
        #[cfg(not(debug_assertions))]
        let format = format.with_target(false);

        let _ = tracing_subscriber::registry()
            .with(format)
            .with(filter)
            .try_init();
    } else {
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
}
