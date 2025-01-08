use crate::bindings::Parameter;

use libc::{c_char, c_void};
use std::ptr::copy;
use std::str::from_utf8;

impl Parameter {
    /// Create new parameter from format code and raw data.
    pub fn new(format: i16, data: &[u8]) -> Self {
        let len = data.len() as i32;
        let ptr = unsafe { libc::malloc(len as usize) as *mut u8 };
        unsafe {
            copy::<u8>(data.as_ptr(), ptr, len as usize);
        }

        Self {
            len,
            data: ptr as *const c_char,
            format: format as i32,
        }
    }

    /// Manually free memory allocated for this parameter.
    ///
    /// SAFETY: call this after plugin finished executing to avoid memory leaks.
    pub fn drop(&mut self) {
        unsafe {
            libc::free(self.data as *mut c_void);
        }
    }

    /// Get parameter value as a string if it's encoded as one.
    pub fn as_str(&self) -> Option<&str> {
        if self.format != 0 {
            return None;
        }

        if let Ok(s) = from_utf8(self.as_bytes()) {
            Some(s)
        } else {
            None
        }
    }

    /// Get parameter value as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        let slice =
            unsafe { core::slice::from_raw_parts(self.data as *const u8, self.len as usize) };

        slice
    }
}
