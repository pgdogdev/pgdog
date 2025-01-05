use super::Query;
use crate::bindings::{self, Route};
use libloading::{library_filename, Library, Symbol};

/// Plugin interface.
#[derive(Debug)]
pub struct Plugin<'a> {
    name: String,
    route: Option<Symbol<'a, unsafe extern "C" fn(bindings::Query) -> Route>>,
}

impl<'a> Plugin<'a> {
    /// Load library using a cross-platform naming convention.
    pub fn library(name: &str) -> Result<Library, libloading::Error> {
        let name = library_filename(name);
        unsafe { Library::new(name) }
    }

    /// Load standard methods from the plugin library.
    pub fn load(name: &str, library: &'a Library) -> Self {
        let route = if let Ok(route) = unsafe { library.get(b"pgdog_route_query\0") } {
            Some(route)
        } else {
            None
        };

        Self {
            name: name.to_owned(),
            route,
        }
    }

    /// Route query.
    pub fn route(&self, query: Query) -> Option<Route> {
        if let Some(route) = &self.route {
            unsafe { Some(route(query.into())) }
        } else {
            None
        }
    }

    /// Plugin name.
    pub fn name(&self) -> &str {
        &self.name
    }
}
