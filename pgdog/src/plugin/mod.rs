//! pgDog plugins.

use std::ops::Deref;

use once_cell::sync::{Lazy, OnceCell};
use pgdog_plugin::libloading::Library;
use pgdog_plugin::Plugin;
use pgdog_plugin::{comp, libloading};
use semver::Version;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

static LIBS: OnceCell<Vec<Library>> = OnceCell::new();
pub static PLUGINS: OnceCell<Vec<Plugin>> = OnceCell::new();
static MIN_VERSION: Lazy<Version> = Lazy::new(|| Version::parse("0.1.9").unwrap());

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

            // Check pgdog-plugin version.
            if let Some(lib_version) = plugin.lib_version() {
                let lib_version = lib_version.deref();
                let lib_version = Version::parse(lib_version).unwrap();
                if lib_version < *MIN_VERSION {
                    warn!("skipping plugin \"{}\" because it's using an unsupported version of pgdog-plugin crate ({})",
                        plugin.name(),
                        lib_version
                    );
                    continue;
                }
            } else {
                warn!("skipping plugin \"{}\" because it's using an old version of pgdog-plugin crate",
                    plugin.name(),
                );
                continue;
            }

            if plugin.init() {
                debug!("plugin \"{}\" initialized", name);
            }

            info!(
                "loaded \"{}\" plugin (v{}) [{:.4}ms]",
                name,
                plugin.version().unwrap_or_default().deref(),
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
