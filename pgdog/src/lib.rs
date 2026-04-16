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

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use pgdog_config::{General, LogFormat};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use tracing_throttle::{Policy, SuppressionSummary, TracingRateLimitLayer};

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
pub fn logger() {
    init_logger(None);
}

/// Setup the logger using PgDog configuration.
pub fn logger_with_config(general: &General) {
    init_logger(Some(general));
}

/// Install the log-throttle filter using the configured dedup window and
/// threshold. Must be called from within a tokio runtime — the throttle's
/// summary emitter task is spawned with `tokio::spawn`.
///
/// No-op when throttling is disabled (window or threshold set to 0) or
/// when the logger was not initialized.
pub fn install_log_throttle(general: &General) {
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
            let format = format.with_filter(throttle);

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
            let format = format.with_filter(throttle);

            let _ = tracing_subscriber::registry()
                .with(format)
                .with(filter)
                .try_init();
        }
    }
}
