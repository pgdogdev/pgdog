//! Plugin interface.

use libloading::{library_filename, Library, Symbol};

use crate::{PdRoute, PdRouterContext, PdStr};

/// Plugin interface.
#[derive(Debug)]
pub struct Plugin<'a> {
    name: String,
    /// Initialization routine.
    init: Option<Symbol<'a, unsafe extern "C" fn()>>,
    /// Shutdown routine.
    fini: Option<Symbol<'a, unsafe extern "C" fn()>>,
    /// Route query.
    route: Option<Symbol<'a, unsafe extern "C" fn(PdRouterContext) -> PdRoute>>,
    /// Compiler version.
    rustc_version: Option<Symbol<'a, unsafe extern "C" fn() -> PdStr>>,
    /// PgQuery version.
    #[allow(dead_code)]
    pg_query_version: Option<Symbol<'a, unsafe extern "C" fn() -> PdStr>>,
}

impl<'a> Plugin<'a> {
    /// Load library using a cross-platform naming convention.
    pub fn library(name: &str) -> Result<Library, libloading::Error> {
        let name = library_filename(name);
        unsafe { Library::new(name) }
    }

    /// Load standard methods from the plugin library.
    pub fn load(name: &str, library: &'a Library) -> Self {
        let init = unsafe { library.get(b"pgdog_init\0") }.ok();
        let fini = unsafe { library.get(b"pgdog_fini\0") }.ok();
        let route = unsafe { library.get(b"pgdog_route\0") }.ok();
        let rustc_version = unsafe { library.get(b"pgdog_rustc_version\0") }.ok();
        let pg_query_version = unsafe { library.get(b"pgdog_pg_query_version\0") }.ok();

        Self {
            name: name.to_owned(),
            init,
            fini,
            route,
            rustc_version,
            pg_query_version,
        }
    }

    /// Perform initialization.
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

    pub fn fini(&self) {
        if let Some(ref fini) = &self.fini {
            unsafe { fini() }
        }
    }

    pub fn route(&self, context: PdRouterContext) -> Option<PdRoute> {
        if let Some(ref route) = &self.route {
            Some(unsafe { route(context) })
        } else {
            None
        }
    }

    /// Plugin name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get Rust compiler version used to build the plugin.
    pub fn rustc_version(&self) -> Option<PdStr> {
        self.rustc_version
            .as_ref()
            .map(|rustc_version| unsafe { rustc_version() })
    }
}
