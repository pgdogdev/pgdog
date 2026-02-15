//! PgDog's plugin interface.
//!
//! This loads the shared library using [`libloading`] and exposes
//! a safe interface to the plugin's methods.
//!

use std::path::Path;

use libloading::{library_filename, Library, Symbol};

use crate::{PdRoute, PdRouterContext, PdStr};

/// Plugin interface.
///
/// Methods are loaded using `libloading`. If required methods aren't found,
/// the plugin isn't loaded. All optional methods are checked first, before being
/// executed.
///
/// Using this interface is reasonably safe.
///
#[derive(Debug)]
pub struct Plugin<'a> {
    /// Plugin name.
    name: String,
    /// Initialization routine.
    init: Option<Symbol<'a, unsafe extern "C" fn()>>,
    /// Shutdown routine.
    fini: Option<Symbol<'a, unsafe extern "C" fn()>>,
    /// Route query.
    route: Option<Symbol<'a, unsafe extern "C" fn(PdRouterContext, *mut PdRoute)>>,
    /// Compiler version.
    rustc_version: Option<Symbol<'a, unsafe extern "C" fn(*mut PdStr)>>,
    /// Plugin API version.
    pgdog_plugin_api_version: Option<Symbol<'a, unsafe extern "C" fn(*mut PdStr)>>,
    /// Plugin version.
    plugin_version: Option<Symbol<'a, unsafe extern "C" fn(*mut PdStr)>>,
}

impl<'a> Plugin<'a> {
    /// Load plugin's shared library using a cross-platform naming convention.
    ///
    /// Plugin has to be in `LD_LIBRARY_PATH`, in a standard location
    /// for the operating system, or be provided as an absolute or relative path,
    /// including the platform-specific extension.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// use pgdog_plugin::Plugin;
    ///
    /// let plugin_lib = Plugin::library("/home/pgdog/plugin.so").unwrap();
    /// let plugin_lib = Plugin::library("plugin.so").unwrap();
    /// ```
    ///
    pub fn library<P: AsRef<Path>>(name: P) -> Result<Library, libloading::Error> {
        if name.as_ref().extension().is_some() {
            let name = name.as_ref().display().to_string();
            unsafe { Library::new(&name) }
        } else {
            let name = library_filename(name.as_ref());
            unsafe { Library::new(name) }
        }
    }

    /// Load standard plugin methods from the plugin library.
    ///
    /// ### Arguments
    ///
    /// * `name`: Plugin name. Can be any name you want, it's only used for logging.
    /// * `library`: `libloading::Library` reference. Must have the same, ideally static, lifetime as the plugin.
    ///
    pub fn load(name: &str, library: &'a Library) -> Self {
        let init = unsafe { library.get(b"pgdog_init\0") }.ok();
        let fini = unsafe { library.get(b"pgdog_fini\0") }.ok();
        let route = unsafe { library.get(b"pgdog_route\0") }.ok();
        let rustc_version = unsafe { library.get(b"pgdog_rustc_version\0") }.ok();
        let pgdog_plugin_api_version = unsafe { library.get(b"pgdog_plugin_api_version\0") }.ok();
        let plugin_version = unsafe { library.get(b"pgdog_plugin_version\0") }.ok();

        Self {
            name: name.to_owned(),
            init,
            fini,
            route,
            rustc_version,
            pgdog_plugin_api_version,
            plugin_version,
        }
    }

    /// Execute plugin's initialization routine.
    /// Returns true if the route exists and was executed, false otherwise.
    pub fn init(&self) -> bool {
        if let Some(init) = &self.init {
            unsafe {
                init();
            }
            true
        } else {
            false
        }
    }

    /// Execute plugin's shutdown routine.
    pub fn fini(&self) {
        if let Some(ref fini) = &self.fini {
            unsafe { fini() }
        }
    }

    /// Execute plugin's route routine. Determines where a statement should be sent.
    /// Returns a route if the routine is defined, or `None` if not.
    ///
    /// ### Arguments
    ///
    /// * `context`: Statement context created by PgDog's query router.
    ///
    pub fn route(&self, context: PdRouterContext) -> Option<PdRoute> {
        if let Some(ref route) = &self.route {
            let mut output = PdRoute::default();
            unsafe {
                route(context, &mut output as *mut PdRoute);
            }
            Some(output)
        } else {
            None
        }
    }

    /// Returns plugin's name. This  is the same name as what
    /// is passed to [`Plugin::load`] function.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Rust compiler version used to build the plugin.
    /// This version must match the compiler version used to build
    /// PgDog, or the plugin won't be loaded.
    pub fn rustc_version(&self) -> Option<PdStr> {
        let mut output = PdStr::default();
        self.rustc_version.as_ref().map(|rustc_version| unsafe {
            rustc_version(&mut output);
            output
        })
    }

    /// Get plugin API version based on `pgdog-plugin` crate version.
    /// This version must match the version used when building pgdog main executable,
    /// otherwise the plugin won't be loaded.
    pub fn pgdog_plugin_api_version(&self) -> Option<PdStr> {
        let mut output = PdStr::default();
        self.pgdog_plugin_api_version.as_ref().map(|func| unsafe {
            func(&mut output as *mut PdStr);
            output
        })
    }

    /// Get plugin version. It's set in plugin's
    /// `Cargo.toml`.
    pub fn version(&self) -> Option<PdStr> {
        let mut output = PdStr::default();
        self.plugin_version.as_ref().map(|func| unsafe {
            func(&mut output as *mut PdStr);
            output
        })
    }
}
