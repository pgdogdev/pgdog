use crate::bindings::*;
use std::ffi::{c_int, c_void};

#[no_mangle]
pub extern "C" fn pgdog_row_new(num_columns: c_int) -> Row {
    let columns = unsafe { libc::malloc(std::mem::size_of::<RowColumn>() * num_columns as usize) };

    Row {
        num_columns,
        columns: columns as *mut RowColumn,
    }
}

#[no_mangle]
pub extern "C" fn pgdog_row_free(row: Row) {
    unsafe {
        libc::free(row.columns as *mut c_void);
    }
}
