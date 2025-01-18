use std::{
    ffi::{CStr, CString},
    ptr::null_mut,
};

use crate::{OrderBy, OrderByDirection};

impl OrderBy {
    pub(crate) fn drop(&self) {
        if !self.column_name.is_null() {
            unsafe { drop(CString::from_raw(self.column_name)) }
        }
    }

    /// Order by column name.
    pub fn column_name(name: &str, direction: OrderByDirection) -> Self {
        let column_name = CString::new(name.as_bytes()).unwrap();

        Self {
            column_name: column_name.into_raw(),
            column_index: -1,
            direction,
        }
    }

    /// Order by column index.
    pub fn column_index(index: usize, direction: OrderByDirection) -> Self {
        Self {
            column_name: null_mut(),
            column_index: index as i32,
            direction,
        }
    }

    /// Get column name if any.
    pub fn name(&self) -> Option<&str> {
        if self.column_name.is_null() || self.column_index >= 0 {
            None
        } else {
            unsafe { CStr::from_ptr(self.column_name).to_str().ok() }
        }
    }
}
