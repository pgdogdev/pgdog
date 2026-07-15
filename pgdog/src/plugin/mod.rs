//! pgDog plugins.

use once_cell::sync::OnceCell;
use pgdog_config::{Config, LogFormat};
use pgdog_plugin::libloading;
use pgdog_plugin::libloading::Library;
use pgdog_plugin::{Config as PdConfig, PdStr, PluginVtable};
use semver::Version;
use std::collections::HashMap;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

static LIBS: OnceCell<Vec<Library>> = OnceCell::new();
pub static PLUGINS: OnceCell<HashMap<String, &'static PluginVtable>> = OnceCell::new();

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
pub fn load(config: &Config) -> Result<(), libloading::Error> {
    if LIBS.get().is_some() {
        return Ok(());
    };

    let plugins = &config.plugins;

    let libs = plugins
        .iter()
        .filter_map(|plugin| {
            PluginVtable::library(&plugin.name)
                .map_err(|err| error!("plugin \"{}\" failed to load: {:#?}", plugin.name, err))
                .ok()
        })
        .collect();

    let _ = LIBS.set(libs);

    let rustc_version = pgdog_plugin::RUSTC_VERSION;
    let pgdog_plugin_api_version = pgdog_plugin::VERSION;

    let plugin_libs = plugins.iter().enumerate().filter_map(|(i, plugin)| {
        if let Some(lib) = LIBS.get().unwrap().get(i) {
            let now = Instant::now();
            let Some(plugin_lib) = PluginVtable::load(lib) else {
                warn!(
                    "skipping plugin \"{}\" because its vtable could not be loaded",
                    plugin.name,
                );
                return None;
            };

            // Check plugin api version (compare major.minor only)
            if !same_major_minor(&plugin_lib.pgdog_plugin_api_version(), pgdog_plugin_api_version) {
                warn!(
                    "skipping plugin \"{}\" because it was compiled with different plugin API version ({})",
                    plugin.name,
                    &*plugin_lib.pgdog_plugin_api_version()
                );
                return None;
            }


            // Check Rust compiler version.
            if rustc_version != &*plugin_lib.rustc_version() {
                warn!(
                    "skipping plugin \"{}\" because it was compiled with different compiler version ({})",
                    plugin.name,
                    &*plugin_lib.rustc_version()
                );
                return None;
            }

            let plugin_config_path = plugin
                .config
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default();

            let pd_config = PdConfig {
                log_level: PdStr::from(config.general.log_level.as_str()),
                log_json: matches!(
                    config.general.log_format,
                    LogFormat::Json | LogFormat::JsonFlattened
                ),
                plugin_config: PdStr::from(plugin_config_path.as_str()),
            };

            plugin_lib.logging_init(pd_config);

            plugin_lib.init();
            debug!("plugin \"{}\" initialized", plugin.name);

            if !plugin_lib.config(pd_config) {
                warn!(
                    "plugin {} failed to load its configuration, skipping",
                    plugin.name
                );
                return None;
            }

            info!(
                "loaded \"{}\" plugin (v{}) [{:.4}ms]",
                plugin.name,
                &*plugin_lib.plugin_version(),
                now.elapsed().as_secs_f64() * 1000.0
            );

            Some((plugin.name.to_owned(), plugin_lib))
        } else {
            None
        }
    }).collect();

    let _ = PLUGINS.set(plugin_libs);

    Ok(())
}

/// Shutdown plugins.
pub fn shutdown() {
    if let Some(plugins) = plugins() {
        for plugin in plugins.values() {
            plugin.fini();
        }
    }
}

/// Get plugin by name.
pub fn plugin(name: &str) -> Option<&PluginVtable> {
    PLUGINS.get().unwrap().get(name).copied()
}

/// Get all loaded plugins.
pub fn plugins() -> Option<&'static HashMap<String, &'static PluginVtable>> {
    PLUGINS.get()
}

/// Load plugins from config.
pub fn load_from_config() -> Result<(), libloading::Error> {
    let config = crate::config::config();

    load(&config.config)
}
