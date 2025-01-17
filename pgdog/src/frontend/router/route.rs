//! Convert `pgdog_plugin::Route` to a route which is [`Send`].

#![allow(non_upper_case_globals)]
use pgdog_plugin::{
    Affinity, Affinity_READ, Affinity_UNKNOWN, Affinity_WRITE, OrderByDirection_ASCENDING,
    OrderByDirection_DESCENDING,
};

#[derive(Copy, Clone, Debug)]
pub enum OrderBy {
    Asc(usize),
    Desc(usize),
}

impl OrderBy {
    /// ORDER BY x ASC
    pub fn asc(&self) -> bool {
        match self {
            OrderBy::Asc(_) => true,
            _ => false,
        }
    }

    /// Column index.
    pub fn index(&self) -> usize {
        match self {
            OrderBy::Asc(column) => *column,
            OrderBy::Desc(column) => *column,
        }
    }
}

/// Query route.
#[derive(Clone, Debug)]
pub struct Route {
    shard: Option<usize>,
    all_shards: bool,
    affinity: Affinity,
    order_by: Vec<OrderBy>,
}

impl Default for Route {
    fn default() -> Self {
        Route::unknown()
    }
}

impl Route {
    /// Get shard if any.
    pub fn shard(&self) -> Option<usize> {
        self.shard
    }

    /// Should this query go to all shards?
    pub fn is_all_shards(&self) -> bool {
        self.all_shards
    }

    /// We don't know where the query should go.
    pub fn unknown() -> Self {
        Self {
            shard: None,
            all_shards: false,
            affinity: Affinity_UNKNOWN,
            order_by: vec![],
        }
    }

    /// The query can be served by a read replica.
    pub fn is_read(&self) -> bool {
        self.affinity == Affinity_READ
    }

    /// The query must be served by a primary.
    pub fn is_write(&self) -> bool {
        self.affinity == Affinity_WRITE
    }

    /// Create new write route for the given shard.
    pub fn write(shard: usize) -> Self {
        Self {
            shard: Some(shard),
            affinity: Affinity_WRITE,
            all_shards: false,
            order_by: vec![],
        }
    }

    /// Get ORDER BY columns.
    pub fn order_by(&self) -> &[OrderBy] {
        &self.order_by
    }
}

impl From<pgdog_plugin::OrderBy> for OrderBy {
    fn from(value: pgdog_plugin::OrderBy) -> Self {
        match value.direction {
            OrderByDirection_ASCENDING => OrderBy::Asc(value.column as usize),
            OrderByDirection_DESCENDING => OrderBy::Desc(value.column as usize),
            _ => unreachable!("OrderByDirection enum can only be ASCENDING or DESCENDING"),
        }
    }
}

impl From<pgdog_plugin::Route> for Route {
    fn from(value: pgdog_plugin::Route) -> Self {
        let all_shards = value.is_all_shards();
        let shard = value.shard().map(|s| s as usize);
        let affinity = value.affinity;
        let mut order_by = vec![];

        for i in 0..value.num_order_by {
            let column = unsafe { value.order_by.offset(i as isize) };
            order_by.push(unsafe { *column }.into());
        }

        Route {
            all_shards,
            shard,
            affinity,
            order_by,
        }
    }
}
