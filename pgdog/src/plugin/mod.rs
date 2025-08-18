//! pgDog plugins.

use std::ops::Deref;

use once_cell::sync::OnceCell;
use pgdog_plugin::libloading::Library;
use pgdog_plugin::Plugin;
use pgdog_plugin::{comp, libloading};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

static LIBS: OnceCell<Vec<Library>> = OnceCell::new();
pub static PLUGINS: OnceCell<Vec<Plugin>> = OnceCell::new();

/// Load plugins.
///
/// # Safety
///
/// This should be run before Tokio is loaded since this is not thread-safe.
///
pub fn load(names: &[&str]) -> Result<(), libloading::Error> {
    if LIBS.get().is_some() {
        return Ok(());
    };

    let mut libs = vec![];
    for plugin in names.iter() {
        match Plugin::library(plugin) {
            Ok(plugin) => libs.push(plugin),
            Err(err) => {
                error!("plugin \"{}\" failed to load: {:#?}", plugin, err);
            }
        }
    }

    let _ = LIBS.set(libs);

    let rustc_version = comp::rustc_version();
    let pg_query_version = env!("PGDOG_PGQUERY_VERSION");

    let mut plugins = vec![];
    for (i, name) in names.iter().enumerate() {
        if let Some(lib) = LIBS.get().unwrap().get(i) {
            let now = Instant::now();
            let plugin = Plugin::load(name, lib);

            // Check Rust compiler version.
            if let Some(plugin_rustc) = plugin.rustc_version() {
                if rustc_version != plugin_rustc {
                    warn!("skipping plugin \"{}\" because it was compiled with different compiler version ({})",
                        plugin.name(),
                        plugin_rustc.deref()
                    );
                    continue;
                }
            } else {
                warn!(
                    "skipping plugin \"{}\" because it doesn't expose its Rust compiler version",
                    plugin.name()
                );
                continue;
            }

            // Check pg_query version.
            if let Some(plugin_pgquery) = plugin.pg_query_version() {
                if pg_query_version != plugin_pgquery.deref() {
                    warn!(
                        "skipping plugin \"{}\" because it has a different pg_query version ({})",
                        plugin.name(),
                        plugin_pgquery.deref()
                    );
                    continue;
                }
            } else {
                warn!(
                    "skipping plugin \"{}\"  because it doesn't expose its pg_query version",
                    plugin.name()
                );
                continue;
            }

            if plugin.init() {
                debug!("plugin \"{}\" initialized", name);
            }

            info!(
                "loaded \"{}\" plugin (v{}, pg_query: v{}) [{:.4}ms]",
                name,
                plugin.version().unwrap_or_default().deref(),
                plugin.pg_query_version().unwrap_or_default().deref(),
                now.elapsed().as_secs_f64() * 1000.0
            );

            plugins.push(plugin);
        }
    }

    let _ = PLUGINS.set(plugins);

    Ok(())
}

/// Shutdown plugins.
pub fn shutdown() {
    if let Some(plugins) = plugins() {
        for plugin in plugins {
            plugin.fini();
        }
    }
}

/// Get plugin by name.
pub fn plugin(name: &str) -> Option<&Plugin<'_>> {
    PLUGINS
        .get()
        .unwrap()
        .iter()
        .find(|&plugin| plugin.name() == name)
}

/// Get all loaded plugins.
pub fn plugins() -> Option<&'static Vec<Plugin<'static>>> {
    PLUGINS.get()
}

/// Load plugins from config.
pub fn load_from_config() -> Result<(), libloading::Error> {
    let config = crate::config::config();

    let plugins = &config
        .config
        .plugins
        .iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>();

    load(plugins)
}
