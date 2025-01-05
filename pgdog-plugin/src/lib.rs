//! pgDog plugin interface.

#[allow(non_upper_case_globals)]
pub mod bindings;

pub mod c_api;
pub mod plugin;
pub mod query;
pub mod route;

pub use c_api::*;
pub use plugin::*;
pub use query::*;

/// Routing decision returned by a plugin.
pub use bindings::Route;

pub use libloading;

pub use bindings::{
    Affinity_READ, Affinity_WRITE, Row, RowColumn, RowDescription, RowDescriptionColumn,
};

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
