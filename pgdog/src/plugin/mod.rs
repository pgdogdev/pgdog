//! pgDog plugins.

use std::ops::Deref;

use once_cell::sync::OnceCell;
use pgdog_plugin::libloading::Library;
use pgdog_plugin::Plugin;
use pgdog_plugin::{comp, libloading};
use semver::Version;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

static LIBS: OnceCell<Vec<Library>> = OnceCell::new();
pub static PLUGINS: OnceCell<Vec<Plugin>> = OnceCell::new();

// Compare semantic versions by major and minor only (ignore patch/bugfix).
fn same_major_minor(a: &str, b: &str) -> bool {
    match (Version::parse(a), Version::parse(b)) {
        (Ok(va), Ok(vb)) => va.major == vb.major && va.minor == vb.minor,
        _ => {
            warn!("failed to parse plugin API version(s) ('{a}' or '{b}'), skipping plugin",);
            false
        }
    }
}

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
    let pgdog_plugin_api_version = comp::pgdog_plugin_api_version();

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

            // Check plugin api version (compare major.minor only)
            if let Some(plugin_api_version) = plugin.pgdog_plugin_api_version() {
                if !same_major_minor(plugin_api_version.deref(), pgdog_plugin_api_version.deref()) {
                    warn!(
                        "skipping plugin \"{}\" because it was compiled with different plugin API version ({})",
                        plugin.name(),
                        plugin_api_version.deref()
                    );
                    continue;
                }
            } else {
                warn!(
                    "plugin {} doesn't expose its plugin API version, please update version of pgdog-plugin crate in your plugin", plugin.name()
                );

                // TODO: use this after after some time when we want to force version
                // compatibility based on pgdog-plugin version
                // warn!(
                //     "skipping plugin \"{}\" because it doesn't expose its plugin API version",
                //     plugin.name()
                // );
                // continue;
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
