//! pgDog plugins.

use once_cell::sync::OnceCell;
use pgdog_plugin::libloading;
use pgdog_plugin::libloading::Library;
use pgdog_plugin::Plugin;
use tracing::info;

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
        let library = Plugin::library(plugin)?;
        libs.push(library);
    }

    let _ = LIBS.set(libs);

    let mut plugins = vec![];
    for (i, name) in names.iter().enumerate() {
        let plugin = Plugin::load(name, LIBS.get().unwrap().get(i).unwrap());
        plugins.push(plugin);
        info!("Loaded \"{}\" plugin", name);
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

#[cfg(test)]
mod test {
    use pgdog_plugin::FfiQuery;

    use super::*;

    #[test]
    fn test_plugin() {
        load(&["routing_plugin", "routing_plugin_c"]).unwrap();
        let query = FfiQuery::new("SELECT 1").unwrap();
        let plug = plugin("routing_plugin_c").unwrap();
        let route = plug.route(query.query()).unwrap();
        assert!(route.read());
        assert_eq!(route.shard(), None);
    }
}
