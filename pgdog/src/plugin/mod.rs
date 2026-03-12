//! pgDog plugins.

use std::ops::Deref;

use once_cell::sync::OnceCell;
use pgdog_plugin::libloading::Library;
use pgdog_plugin::{comp, libloading};
use pgdog_plugin::{PdStr, Plugin};
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
pub fn load(plugins: &[pgdog_config::Plugin]) -> Result<(), libloading::Error> {
    if LIBS.get().is_some() {
        return Ok(());
    };

    let mut libs = vec![];
    for plugin in plugins {
        match Plugin::library(&plugin.name) {
            Ok(plugin) => libs.push(plugin),
            Err(err) => {
                error!("plugin \"{}\" failed to load: {:#?}", plugin.name, err);
            }
        }
    }

    let _ = LIBS.set(libs);

    let rustc_version = comp::rustc_version();
    let pgdog_plugin_api_version = comp::pgdog_plugin_api_version();

    let mut plugin_libs = vec![];
    for (i, plugin) in plugins.iter().enumerate() {
        if let Some(lib) = LIBS.get().unwrap().get(i) {
            let now = Instant::now();
            let plugin_lib = Plugin::load(&plugin.name, lib);

            // Check Rust compiler version.
            if let Some(plugin_rustc) = plugin_lib.rustc_version() {
                if rustc_version != plugin_rustc {
                    warn!("skipping plugin \"{}\" because it was compiled with different compiler version ({})",
                        plugin_lib.name(),
                        plugin_rustc.deref()
                    );
                    continue;
                }
            } else {
                warn!(
                    "skipping plugin \"{}\" because it doesn't expose its Rust compiler version",
                    plugin_lib.name()
                );
                continue;
            }

            // Check plugin api version (compare major.minor only)
            if let Some(plugin_api_version) = plugin_lib.pgdog_plugin_api_version() {
                if !same_major_minor(plugin_api_version.deref(), pgdog_plugin_api_version.deref()) {
                    warn!(
                        "skipping plugin \"{}\" because it was compiled with different plugin API version ({})",
                        plugin_lib.name(),
                        plugin_api_version.deref()
                    );
                    continue;
                }
            } else {
                warn!(
                    "plugin {} doesn't expose its plugin API version, please update version of pgdog-plugin crate in your plugin", plugin_lib.name()
                );

                // TODO: use this after after some time when we want to force version
                // compatibility based on pgdog-plugin version
                // warn!(
                //     "skipping plugin \"{}\" because it doesn't expose its plugin API version",
                //     plugin.name()
                // );
                // continue;
            }

            if plugin_lib.init() {
                debug!("plugin \"{}\" initialized", plugin_lib.name());
            }

            if let Some(ref config) = plugin.config {
                let config_path = config.display().to_string();
                let config_str = PdStr::from(config_path.as_str());

                if !plugin_lib.config(config_str) {
                    warn!(
                        "plugin {} failed to load its configuration file, skipping",
                        plugin_lib.name()
                    );
                    continue;
                }
            }

            info!(
                "loaded \"{}\" plugin (v{}) [{:.4}ms]",
                plugin_lib.name(),
                plugin_lib.version().unwrap_or_default().deref(),
                now.elapsed().as_secs_f64() * 1000.0
            );

            plugin_libs.push(plugin_lib);
        }
    }

    let _ = PLUGINS.set(plugin_libs);

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

    load(&config.config.plugins)
}
