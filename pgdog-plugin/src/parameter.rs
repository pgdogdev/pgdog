use crate::bindings::Parameter;

use libc::c_char;
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::copy;
use std::slice::from_raw_parts;
use std::str::from_utf8;

impl Parameter {
    /// Create new parameter from format code and raw data.
    pub fn new(format: i16, data: &[u8]) -> Self {
        let len = data.len() as i32;
        let layout = Layout::array::<u8>(len as usize).unwrap();
        let ptr = unsafe { alloc(layout) };
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
    /// # Safety
    ///
    /// Call this after plugin finished executing to avoid memory leaks.
    pub unsafe fn deallocate(&mut self) {
        let layout = Layout::array::<u8>(self.len as usize).unwrap();
        unsafe {
            dealloc(self.data as *mut u8, layout);
        }
    }

    /// Get parameter value as a string if it's encoded as one.
    pub fn as_str(&self) -> Option<&str> {
        if self.format != 0 {
            return None;
        }

        from_utf8(self.as_bytes()).ok()
    }

    /// Get parameter value as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        let slice = unsafe { from_raw_parts(self.data as *const u8, self.len as usize) };

        slice
    }
}
