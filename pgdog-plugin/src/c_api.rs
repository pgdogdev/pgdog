use crate::bindings::*;
use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    ffi::c_int,
};

#[no_mangle]
pub extern "C" fn pgdog_row_new(num_columns: c_int) -> Row {
    let layout = Layout::from_size_align(
        num_columns as usize * std::mem::size_of::<RowColumn>(),
        std::mem::align_of::<RowColumn>(),
    )
    .unwrap();
    let row = Row {
        num_columns,
        columns: unsafe { alloc_zeroed(layout) as *mut RowColumn },
    };

    row
}

#[no_mangle]
pub extern "C" fn pgdog_row_free(row: Row) {
    let layout = Layout::from_size_align(
        row.num_columns as usize * std::mem::size_of::<RowColumn>(),
        std::mem::align_of::<RowColumn>(),
    )
    .unwrap();

    unsafe {
        dealloc(row.columns as *mut u8, layout);
    }
}
