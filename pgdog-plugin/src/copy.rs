//! Handle COPY commands.

use crate::bindings::{CopyInput, CopyOutput, CopyRow};
use std::{
    alloc::{alloc, dealloc, Layout},
    ffi::{c_char, CStr, CString},
    ptr::{copy, null_mut, slice_from_raw_parts},
    slice::from_raw_parts,
    str::from_utf8_unchecked,
};

impl CopyInput {
    /// Create new copy input.
    pub fn new(data: &[u8], sharding_column: usize, shards: usize, headers: bool) -> Self {
        Self {
            len: data.len() as i32,
            data: data.as_ptr() as *const i8,
            sharding_column: sharding_column as i32,
            num_shards: shards as i32,
            headers: if headers { 1 } else { 0 },
        }
    }

    /// Get data as slice.
    pub fn data(&self) -> &[u8] {
        unsafe { from_raw_parts(self.data as *const u8, self.len as usize) }
    }
}

impl CopyRow {
    /// Create new row from data slice.
    pub fn new(data: &[u8], shard: i32) -> Self {
        Self {
            len: data.len() as i32,
            data: data.as_ptr() as *mut i8,
            shard,
        }
    }

    /// Get data.
    pub fn data(&self) -> &[u8] {
        unsafe { from_raw_parts(self.data as *const u8, self.len as usize) }
    }
}

impl std::fmt::Debug for CopyRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyRow")
            .field("len", &self.len)
            .field("shard", &self.shard)
            .field("data", &unsafe { from_utf8_unchecked(self.data()) })
            .finish()
    }
}

impl CopyOutput {
    /// Copy output from rows.
    pub fn new(rows: &[CopyRow]) -> Self {
        let layout = Layout::array::<CopyRow>(rows.len()).unwrap();
        unsafe {
            let ptr = alloc(layout) as *mut CopyRow;
            copy(rows.as_ptr(), ptr, rows.len());
            Self {
                num_rows: rows.len() as i32,
                rows: ptr,
                header: null_mut(),
            }
        }
    }

    /// Parse and give back the CSV header.
    pub fn with_header(mut self, header: Option<String>) -> Self {
        if let Some(header) = header {
            let ptr = CString::new(header).unwrap().into_raw();
            self.header = ptr;
        }

        self
    }

    /// Get rows.
    pub fn rows(&self) -> &[CopyRow] {
        unsafe { from_raw_parts(self.rows, self.num_rows as usize) }
    }

    /// Get header value, if any.
    pub fn header(&self) -> Option<&str> {
        unsafe {
            if !self.header.is_null() {
                CStr::from_ptr(self.header).to_str().ok()
            } else {
                None
            }
        }
    }

    /// Deallocate this structure.
    ///
    /// # Safety
    ///
    /// Don't use unless you don't need this data anymore.
    ///
    pub unsafe fn deallocate(&self) {
        let layout = Layout::array::<CopyRow>(self.num_rows as usize).unwrap();
        dealloc(self.rows as *mut u8, layout);

        if !self.header.is_null() {
            unsafe { drop(CString::from_raw(self.header)) }
        }
    }
}

impl std::fmt::Debug for CopyOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rows = (0..self.num_rows)
            .map(|i| unsafe { *self.rows.offset(i as isize) })
            .collect::<Vec<_>>();

        f.debug_struct("CopyOutput")
            .field("num_rows", &self.num_rows)
            .field("rows", &rows)
            .finish()
    }
}
