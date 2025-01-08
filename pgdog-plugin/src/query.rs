use crate::bindings::{self, Parameter};

use std::alloc::{alloc, dealloc, Layout};
use std::ffi::{c_char, CStr, CString};
use std::marker::PhantomData;
use std::ptr::{copy, null};

/// Rust-safe [`bindings::Query`] wrapper.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct Query<'a> {
    /// Length of the query string.
    len: usize,
    /// Query string.
    query: *const c_char,
    /// Number of parameters if any.
    num_parameters: usize,
    parameters: *const bindings::Parameter,
    /// Lifetime marker ensuring that the CString
    /// from which this query is created is not deallocated too soon.
    _lifetime: PhantomData<&'a ()>,
    /// This instance owns the allocated data.
    owned: bool,
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
            num_parameters: value.num_parameters as i32,
            parameters: value.parameters,
        }
    }
}

impl From<bindings::Query> for Query<'_> {
    fn from(value: bindings::Query) -> Self {
        Self {
            len: value.len as usize,
            query: value.query as *const c_char,
            num_parameters: value.num_parameters as usize,
            parameters: value.parameters,
            _lifetime: PhantomData,
            owned: true,
        }
    }
}

impl<'a> Query<'a> {
    /// Get query text.
    pub fn query(&self) -> &str {
        debug_assert!(self.query != null());
        unsafe { CStr::from_ptr(self.query) }.to_str().unwrap()
    }

    /// Create new query to pass it over the FFI boundary.
    pub fn new(query: &'a CString) -> Query<'a> {
        Self {
            len: query.as_bytes().len(),
            query: query.as_ptr() as *const c_char,
            num_parameters: 0,
            parameters: null(),
            _lifetime: PhantomData,
            owned: true,
        }
    }

    /// Add parameters.
    pub fn parameters(&mut self, params: &[Parameter]) {
        let layout = Layout::array::<Parameter>(params.len()).unwrap();
        let parameters = unsafe { alloc(layout) };

        unsafe {
            copy(params.as_ptr(), parameters as *mut Parameter, params.len());
        }
        self.parameters = parameters as *const Parameter;
        self.num_parameters = params.len();
    }

    /// Get parameter at offset if one exists.
    pub fn parameter(&self, index: usize) -> Option<Parameter> {
        if index < self.num_parameters {
            unsafe { Some(*(self.parameters.offset(index as isize))) }
        } else {
            None
        }
    }

    /// Free memory allocated for parameters, if any.
    pub fn drop(&mut self) {
        if !self.parameters.is_null() {
            for index in 0..self.num_parameters {
                if let Some(mut param) = self.parameter(index) {
                    param.drop();
                }
            }
            let layout = Layout::array::<Parameter>(self.num_parameters).unwrap();
            unsafe {
                dealloc(self.parameters as *mut u8, layout);
                self.parameters = null();
            }
        }
    }
}
