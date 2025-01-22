use super::OrderBy;

/// Path a query should take.
#[derive(Debug, Clone)]
pub struct Route {
    shard: Option<usize>,
    read: bool,
    order_by: Vec<OrderBy>,
}

impl Default for Route {
    fn default() -> Self {
        Self::write(None)
    }
}

impl Route {
    pub fn select(shard: Option<usize>, order_by: &[OrderBy]) -> Self {
        Self {
            shard,
            order_by: order_by.to_vec(),
            read: true,
        }
    }

    pub fn read(shard: Option<usize>) -> Self {
        Self {
            shard,
            read: true,
            order_by: vec![],
        }
    }

    pub fn write(shard: Option<usize>) -> Self {
        Self {
            shard,
            read: false,
            order_by: vec![],
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

    pub fn overwrite_shard(&mut self, shard: usize) {
        self.shard = Some(shard);
    }
}
