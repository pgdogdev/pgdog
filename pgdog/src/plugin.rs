//! pgDog plugins.

use once_cell::sync::OnceCell;
use pgdog_plugin::libloading;
use pgdog_plugin::libloading::Library;
use pgdog_plugin::Plugin;
use tracing::{error, info, warn};

static LIBS: OnceCell<Vec<Library>> = OnceCell::new();
pub static PLUGINS: OnceCell<Vec<Plugin>> = OnceCell::new();

/// Load plugins.
///
/// # Safety
///
/// This should be run before Tokio is loaded since this is not thread-safe.
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

    let mut plugins = vec![];
    for (i, name) in names.iter().enumerate() {
        if let Some(lib) = LIBS.get().unwrap().get(i) {
            let plugin = Plugin::load(name, lib);

            if !plugin.valid() {
                warn!("plugin \"{}\" is missing required symbols, skipping", name);
            } else {
                plugins.push(plugin);
                info!("Loaded \"{}\" plugin", name);
            }
        }
    }

    let _ = PLUGINS.set(plugins);

    Ok(())
}

/// Get plugin by name.
pub fn plugin(name: &str) -> Option<&Plugin> {
    for plugin in PLUGINS.get().unwrap() {
        if plugin.name() == name {
            return Some(plugin);
        }
    }

    None
}

/// Get all loaded plugins.
pub fn plugins() -> &'static Vec<Plugin<'static>> {
    PLUGINS.get().unwrap()
}

/// Load plugins from config.
pub fn load_from_config() -> Result<(), libloading::Error> {
    let config = crate::config::config();

    let plugins = &config
        .general
        .plugins
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>();

    load(plugins)
}

#[cfg(test)]
mod test {
    use pgdog_plugin::FfiQuery;

    use super::*;

    #[test]
    fn test_plugin() {
        use tracing_subscriber::{fmt, prelude::*, EnvFilter};
        let _ = tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .try_init();

        load(&["routing_plugin", "routing_plugin_c"]).unwrap();
        let query = FfiQuery::new("SELECT 1").unwrap();
        let plug = plugin("routing_plugin_c").unwrap();
        let route = plug.route(query.query()).unwrap();
        assert!(route.read());
        assert_eq!(route.shard(), None);
    }
}
