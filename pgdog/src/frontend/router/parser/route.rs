use super::{Aggregate, OrderBy};

/// Path a query should take and any transformations
/// that should be applied along the way.
#[derive(Debug, Clone)]
pub struct Route {
    shard: Option<usize>,
    read: bool,
    order_by: Vec<OrderBy>,
    aggregate: Aggregate,
}

impl Default for Route {
    fn default() -> Self {
        Self::write(None)
    }
}

impl Route {
    /// SELECT query.
    pub fn select(shard: Option<usize>, order_by: &[OrderBy], aggregate: &Aggregate) -> Self {
        Self {
            shard,
            order_by: order_by.to_vec(),
            read: true,
            aggregate: aggregate.clone(),
        }
    }

    /// A query that should go to a replica.
    pub fn read(shard: Option<usize>) -> Self {
        Self {
            shard,
            read: true,
            order_by: vec![],
            aggregate: Aggregate::default(),
        }
    }

    /// A write query.
    pub fn write(shard: Option<usize>) -> Self {
        Self {
            shard,
            read: false,
            order_by: vec![],
            aggregate: Aggregate::default(),
        }
    }

    pub fn is_read(&self) -> bool {
        self.read
    }

    pub fn is_write(&self) -> bool {
        !self.is_read()
    }

    /// Get shard if any.
    pub fn shard(&self) -> Option<usize> {
        self.shard
    }

    /// Should this query go to all shards?
    pub fn is_all_shards(&self) -> bool {
        self.shard.is_none()
    }

    pub fn order_by(&self) -> &[OrderBy] {
        &self.order_by
    }

    pub fn aggregate(&self) -> &Aggregate {
        &self.aggregate
    }

    pub fn set_shard(&mut self, shard: usize) {
        self.shard = Some(shard);
    }

    pub fn should_buffer(&self) -> bool {
        !self.order_by().is_empty() || !self.aggregate().is_empty()
    }
}
