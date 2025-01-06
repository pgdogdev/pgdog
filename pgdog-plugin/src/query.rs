use crate::bindings::{self};

use std::ffi::{c_char, CStr, CString, NulError};
use std::marker::PhantomData;
use std::ptr::null;

/// Rust-safe [`bindings::Query`] wrapper.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct Query<'a> {
    /// Length of the query string.
    len: usize,
    /// Query string.
    query: *const c_char,
    /// Number of parameters if any.
    num_values: usize,
    values: *const bindings::Value,
    /// Lifetime marker ensuring that the CString
    /// from which this query is created is not deallocated too soon.
    _lifetime: PhantomData<&'a ()>,
}

impl std::fmt::Debug for Query<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.query())
    }
}

impl From<Query<'_>> for bindings::Query {
    fn from(value: Query<'_>) -> Self {
        Self {
            len: value.len as i32,
            query: value.query as *mut i8,
            num_values: 0,
            values: null(),
        }
    }
}

impl From<bindings::Query> for Query<'_> {
    fn from(value: bindings::Query) -> Self {
        Self {
            len: value.len as usize,
            query: value.query as *const c_char,
            num_values: 0,
            values: null(),
            _lifetime: PhantomData,
        }
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
            num_values: 0,
            values: null(),
            _lifetime: PhantomData,
        }
    }
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
