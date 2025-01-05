//! pgDog plugin interface.

mod bindings;

use libloading::{library_filename, Library, Symbol};
use std::{
    ffi::{c_int, CStr, CString, NulError},
    fmt::Debug,
    marker::PhantomData,
    os::raw::c_char,
    ptr::null,
};

pub use libloading;

/// Query executed through the pooler.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct Query<'a> {
    /// Length of the query string.
    len: usize,
    /// Query string.
    query: *const c_char,
    /// Lifetime marker.
    _lifetime: PhantomData<&'a ()>,
}

impl Debug for Query<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.query())
    }
}

impl<'a> Query<'a> {
    /// Get query text.
    pub fn query(&self) -> &str {
        assert!(self.query != null());
        unsafe { CStr::from_ptr(self.query) }.to_str().unwrap()
    }

    /// Create new query to pass it over the FFI boundary.
    pub fn new(query: &'a CString) -> Query<'a> {
        Self {
            len: query.as_bytes().len(),
            query: query.as_ptr() as *const c_char,
            _lifetime: PhantomData,
        }
    }
}

#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct Route {
    /// Does it want a read or a write?
    pub affinity: Affinity,
    /// Which shard, if any. -1 = any shard.
    pub shard: c_int,
}

impl Route {
    /// Is this a read?
    pub fn read(&self) -> bool {
        self.affinity == Affinity::Read
    }

    /// Is this a write?
    pub fn write(&self) -> bool {
        self.affinity == Affinity::Write
    }

    /// Which shard, if any.
    pub fn shard(&self) -> Option<usize> {
        if self.shard < 0 {
            None
        } else {
            Some(self.shard as usize)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(C)]
pub enum Affinity {
    Read = 1,
    Write = 2,
}

/// FFI-safe Rust query.
#[derive(Debug, Clone, PartialEq)]
pub struct FfiQuery {
    query: CString,
}

impl FfiQuery {
    /// Construct a query that will survive the FFI boundary.
    pub fn new(query: &str) -> Result<Self, NulError> {
        let query = CString::new(query)?;
        Ok(Self { query })
    }

    /// Get the FFI-safe query struct.
    pub fn query(&self) -> Query {
        Query::new(&self.query)
    }
}

/// Plugin interface.
#[derive(Debug)]
pub struct Plugin<'a> {
    name: String,
    route: Option<Symbol<'a, unsafe extern "C" fn(Query) -> Route>>,
}

impl<'a> Plugin<'a> {
    /// Load library using a cross-platform naming convention.
    pub fn library(name: &str) -> Result<Library, libloading::Error> {
        let name = library_filename(name);
        unsafe { Library::new(name) }
    }

    /// Load standard methods from the plugin library.
    pub fn load(name: &str, library: &'a Library) -> Self {
        let route = if let Ok(route) = unsafe { library.get(b"route\0") } {
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
            unsafe { Some(route(query)) }
        } else {
            None
        }
    }

    /// Plugin name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_query() {
        let query = CString::new("SELECT 1").unwrap();
        let query = Query::new(&query);
        assert_eq!(query.query(), "SELECT 1");
    }
}
