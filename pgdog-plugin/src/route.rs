//! Query routing helpers.
#![allow(non_upper_case_globals)]

use std::{
    alloc::{alloc, dealloc, Layout},
    ptr::{copy, null_mut},
};

use crate::bindings::*;

impl RoutingOutput {
    /// Create new route.
    pub fn new_route(route: Route) -> RoutingOutput {
        RoutingOutput { route }
    }

    /// Create new copy statement.
    pub fn new_copy(copy: Copy) -> RoutingOutput {
        RoutingOutput { copy }
    }

    /// Create new copy rows output.
    pub fn new_copy_rows(copy_rows: CopyOutput) -> RoutingOutput {
        RoutingOutput { copy_rows }
    }
}

impl Route {
    /// The plugin has no idea what to do with this query.
    /// The router will ignore this and try another way.
    pub fn unknown() -> Route {
        Route {
            shard: Shard_ANY,
            affinity: Affinity_UNKNOWN,
            num_order_by: 0,
            order_by: null_mut(),
        }
    }

    /// Read from this shard.
    pub fn read(shard: usize) -> Route {
        Route {
            shard: shard as i32,
            affinity: Affinity_READ,
            num_order_by: 0,
            order_by: null_mut(),
        }
    }

    /// Write to this shard.
    pub fn write(shard: usize) -> Route {
        Route {
            shard: shard as i32,
            affinity: Affinity_WRITE,
            num_order_by: 0,
            order_by: null_mut(),
        }
    }

    /// Read from any shard.
    pub fn read_any() -> Self {
        Self {
            affinity: Affinity_READ,
            shard: Shard_ANY,
            num_order_by: 0,
            order_by: null_mut(),
        }
    }

    /// Read from all shards.
    pub fn read_all() -> Self {
        Self {
            affinity: Affinity_READ,
            shard: Shard_ALL,
            num_order_by: 0,
            order_by: null_mut(),
        }
    }

    /// Read from any shard.
    pub fn write_any() -> Self {
        Self {
            affinity: Affinity_WRITE,
            shard: Shard_ANY,
            num_order_by: 0,
            order_by: null_mut(),
        }
    }

    /// Write to all shards.
    pub fn write_all() -> Self {
        Self {
            affinity: Affinity_WRITE,
            shard: Shard_ALL,
            num_order_by: 0,
            order_by: null_mut(),
        }
    }

    /// Is this a read?
    pub fn is_read(&self) -> bool {
        self.affinity == Affinity_READ
    }

    /// Is this a write?
    pub fn is_write(&self) -> bool {
        self.affinity == Affinity_WRITE
    }

    /// This query indicates a transaction a starting, e.g. BEGIN.
    pub fn is_transaction_start(&self) -> bool {
        self.affinity == Affinity_TRANSACTION_START
    }

    /// This query indicates a transaction is ending, e.g. COMMIT/ROLLBACK.
    pub fn is_transaction_end(&self) -> bool {
        self.affinity == Affinity_TRANSACTION_END
    }

    /// Which shard, if any.
    pub fn shard(&self) -> Option<usize> {
        if self.shard < 0 {
            None
        } else {
            Some(self.shard as usize)
        }
    }

    /// Can send query to any shard.
    pub fn is_any_shard(&self) -> bool {
        self.shard == Shard_ANY
    }

    /// Send queries to all shards.
    pub fn is_all_shards(&self) -> bool {
        self.shard == Shard_ALL
    }

    /// The plugin has no idea where to route this query.
    pub fn is_unknown(&self) -> bool {
        self.shard == Shard_ANY && self.affinity == Affinity_UNKNOWN
    }

    /// Add order by columns to the route.
    pub fn order_by(&mut self, order_by: &[OrderBy]) {
        let num_order_by = order_by.len();
        let layout = Layout::array::<OrderBy>(num_order_by).unwrap();
        let ptr = unsafe { alloc(layout) as *mut OrderBy };
        unsafe { copy(order_by.as_ptr(), ptr, num_order_by) };
        self.num_order_by = num_order_by as i32;
        self.order_by = ptr;
    }

    /// Deallocate memory.
    ///
    /// # Safety
    ///
    /// Don't use this unless you're cleaning up plugin output.
    pub(crate) unsafe fn deallocate(&self) {
        if self.num_order_by > 0 {
            (0..self.num_order_by).for_each(|index| (*self.order_by.offset(index as isize)).drop());
            let layout = Layout::array::<OrderBy>(self.num_order_by as usize).unwrap();
            dealloc(self.order_by as *mut u8, layout);
        }
    }
}
