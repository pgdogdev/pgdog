//! pgDog plugin interface.

#[allow(non_upper_case_globals)]
pub mod bindings;

pub mod c_api;
pub mod config;
pub mod copy;
pub mod input;
pub mod order_by;
pub mod output;
pub mod parameter;
pub mod plugin;
pub mod query;
pub mod route;

pub use bindings::*;
pub use c_api::*;
pub use plugin::*;

pub use libloading;

#[cfg(test)]
mod test {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_query() {
        let query = CString::new("SELECT 1").unwrap();
        let query = Query::new(query);
        assert_eq!(query.query(), "SELECT 1");
    }
}
