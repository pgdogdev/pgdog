//! Handle COPY commands.

use libc::c_char;

use crate::{
    bindings::{Copy, CopyInput, CopyOutput, CopyRow},
    CopyFormat_CSV, CopyFormat_INVALID,
};
use std::{
    alloc::{alloc, dealloc, Layout},
    ffi::{CStr, CString},
    ptr::{copy, null_mut},
    slice::from_raw_parts,
    str::from_utf8_unchecked,
};

impl Copy {
    /// Not a valid COPY statement. Will be ignored by the router.
    pub fn invalid() -> Self {
        Self {
            copy_format: CopyFormat_INVALID,
            table_name: null_mut(),
            has_headers: 0,
            delimiter: ',' as c_char,
            num_columns: 0,
            columns: null_mut(),
        }
    }

    /// Create new copy command.
    pub fn new(table_name: &str, headers: bool, delimiter: char, columns: &[&str]) -> Self {
        let mut cols = vec![];
        for column in columns {
            let cstr = CString::new(column.as_bytes()).unwrap();
            cols.push(cstr.into_raw());
        }
        let layout = Layout::array::<*mut i8>(columns.len()).unwrap();
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let ptr = unsafe { alloc(layout) as *mut *mut u8 };
        #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
        let ptr = unsafe { alloc(layout) as *mut *mut i8 };
        unsafe {
            copy(cols.as_ptr(), ptr, columns.len());
        }

        Self {
            table_name: CString::new(table_name).unwrap().into_raw(),
            has_headers: if headers { 1 } else { 0 },
            copy_format: CopyFormat_CSV,
            delimiter: delimiter as c_char,
            num_columns: columns.len() as i32,
            columns: ptr,
        }
    }

    /// Get table name.
    pub fn table_name(&self) -> &str {
        unsafe { CStr::from_ptr(self.table_name).to_str().unwrap() }
    }

    /// Does this COPY statement say to expect headers?
    pub fn has_headers(&self) -> bool {
        self.has_headers != 0
    }

    /// Columns specified by the caller.
    pub fn columns(&self) -> Vec<&str> {
        unsafe {
            (0..self.num_columns)
                .map(|s| {
                    CStr::from_ptr(*self.columns.offset(s as isize))
                        .to_str()
                        .unwrap()
                })
                .collect()
        }
    }

    /// Get CSV delimiter.
    pub fn delimiter(&self) -> char {
        self.delimiter as u8 as char
    }

    /// Deallocate this structure.
    ///
    /// # Safety
    ///
    /// Call this only when finished with this.
    ///
    pub unsafe fn deallocate(&self) {
        unsafe { drop(CString::from_raw(self.table_name)) }

        (0..self.num_columns)
            .for_each(|i| drop(CString::from_raw(*self.columns.offset(i as isize))));

        let layout = Layout::array::<*mut i8>(self.num_columns as usize).unwrap();
        unsafe { dealloc(self.columns as *mut u8, layout) }
    }
}

impl CopyInput {
    /// Create new copy input.
    pub fn new(data: &[u8], sharding_column: usize, headers: bool, delimiter: char) -> Self {
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let data_ptr = data.as_ptr() as *const u8;
        #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
        let data_ptr = data.as_ptr() as *const i8;
        Self {
            len: data.len() as i32,
            data: data_ptr,
            sharding_column: sharding_column as i32,
            has_headers: if headers { 1 } else { 0 },
            delimiter: delimiter as c_char,
        }
    }

    /// Get data as slice.
    pub fn data(&self) -> &[u8] {
        unsafe { from_raw_parts(self.data as *const u8, self.len as usize) }
    }

    /// CSV delimiter.
    pub fn delimiter(&self) -> char {
        self.delimiter as u8 as char
    }

    /// Sharding column offset.
    pub fn sharding_column(&self) -> usize {
        self.sharding_column as usize
    }

    /// Does this input contain headers? Only the first one will.
    pub fn headers(&self) -> bool {
        self.has_headers != 0
    }
}

impl CopyRow {
    /// Create new row from data slice.
    pub fn new(data: &[u8], shard: i32) -> Self {
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let data_ptr = data.as_ptr() as *mut u8;
        #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
        let data_ptr = data.as_ptr() as *mut i8;
        Self {
            len: data.len() as i32,
            data: data_ptr,
            shard,
        }
    }

    /// Shard this row should go to.
    pub fn shard(&self) -> usize {
        self.shard as usize
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
