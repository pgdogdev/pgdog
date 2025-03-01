use crate::bindings::{Parameter, Query};

use std::alloc::{alloc, dealloc, Layout};
use std::ffi::{CStr, CString};
use std::ptr::{copy, null};

impl Query {
    /// Get query text.
    pub fn query(&self) -> &str {
        debug_assert!(!self.query.is_null());
        unsafe { CStr::from_ptr(self.query) }.to_str().unwrap()
    }

    /// Create new query to pass it over the FFI boundary.
    pub fn new(query: CString) -> Self {
        Self {
            len: query.as_bytes().len() as i32,
            query: query.into_raw(),
            num_parameters: 0,
            parameters: null(),
        }
    }

    /// Set parameters on this query. This is used internally
    /// by pgDog to construct this structure.
    pub fn set_parameters(&mut self, params: &[Parameter]) {
        let layout = Layout::array::<Parameter>(params.len()).unwrap();
        let parameters = unsafe { alloc(layout) };

        unsafe {
            copy(params.as_ptr(), parameters as *mut Parameter, params.len());
        }
        self.parameters = parameters as *const Parameter;
        self.num_parameters = params.len() as i32;
    }

    /// Get query parameters, if any.
    pub fn parameters(&self) -> Vec<Parameter> {
        (0..self.num_parameters)
            .map(|i| self.parameter(i as usize).unwrap())
            .collect()
    }

    /// Get parameter at offset if one exists.
    pub fn parameter(&self, index: usize) -> Option<Parameter> {
        if index < self.num_parameters as usize {
            unsafe { Some(*self.parameters.add(index)) }
        } else {
            None
        }
    }

    /// Free memory allocated for parameters, if any.
    ///
    /// # Safety
    ///
    /// This is not to be used by plugins.
    /// This is for internal pgDog usage only.
    pub unsafe fn deallocate(&mut self) {
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let ptr = self.query as *mut u8;
        #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
        let ptr = self.query as *mut i8;

        unsafe { drop(CString::from_raw(ptr)) }

        if !self.parameters.is_null() {
            for index in 0..self.num_parameters {
                if let Some(mut param) = self.parameter(index as usize) {
                    param.deallocate();
                }
            }
            let layout = Layout::array::<Parameter>(self.num_parameters as usize).unwrap();
            unsafe {
                dealloc(self.parameters as *mut u8, layout);
                self.parameters = null();
            }
        }
    }
}
