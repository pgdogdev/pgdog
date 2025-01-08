use crate::bindings::*;
use std::alloc::{alloc, dealloc, Layout};
use std::ffi::c_int;

/// Create new row.
#[no_mangle]
pub extern "C" fn pgdog_row_new(num_columns: c_int) -> Row {
    let layout = Layout::array::<RowColumn>(num_columns as usize).unwrap();
    let columns = unsafe { alloc(layout) };

    Row {
        num_columns,
        columns: columns as *mut RowColumn,
    }
}

/// Delete a row.
#[no_mangle]
pub extern "C" fn pgdog_row_free(row: Row) {
    let layout = Layout::array::<RowColumn>(row.num_columns as usize).unwrap();
    unsafe {
        dealloc(row.columns as *mut u8, layout);
    }
}
