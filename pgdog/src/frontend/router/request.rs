//! Memory-safe wrapper around the FFI binding to Query.
use pgdog_plugin::Query;
use std::{
    ffi::CString,
    ops::{Deref, DerefMut},
};

use super::Error;

/// Memory-safe wrapper around the FFI binding to Query.
pub struct Request {
    query: Query,
}

impl Deref for Request {
    type Target = Query;
    fn deref(&self) -> &Self::Target {
        &self.query
    }
}

impl DerefMut for Request {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.query
    }
}

impl Request {
    /// New query request.
    pub fn new(query: &str) -> Result<Self, Error> {
        Ok(Self {
            query: Query::new(CString::new(query.as_bytes())?),
        })
    }

    /// Get constructed query.
    pub fn query(&self) -> Query {
        self.query
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        unsafe { self.query.deallocate() }
    }
}
