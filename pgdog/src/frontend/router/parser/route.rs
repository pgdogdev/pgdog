use std::fmt::Display;

use super::{Aggregate, OrderBy};

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub enum Shard {
    Direct(usize),
    Multi(Vec<usize>),
    All,
}

impl Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Direct(shard) => shard.to_string(),
                Self::Multi(shards) => format!("{:?}", shards),
                Self::All => "all".into(),
            }
        )
    }
}

impl Shard {
    pub fn all(&self) -> bool {
        matches!(self, Shard::All)
    }

    pub fn direct(shard: usize) -> Self {
        Self::Direct(shard)
    }
}

impl From<Option<usize>> for Shard {
    fn from(value: Option<usize>) -> Self {
        if let Some(value) = value {
            Shard::Direct(value)
        } else {
            Shard::All
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Limit {
    pub limit: usize,
    pub offset: usize,
}

/// Path a query should take and any transformations
/// that should be applied along the way.
#[derive(Debug, Clone)]
pub struct Route {
    shard: Shard,
    read: bool,
    order_by: Vec<OrderBy>,
    aggregate: Aggregate,
    limit: Option<Limit>,
}

impl Display for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shard={}, role={}",
            self.shard,
            if self.read { "replica" } else { "primary" }
        )
    }
}

impl Default for Route {
    fn default() -> Self {
        Self::write(None)
    }
}

impl Route {
    /// SELECT query.
    pub fn select(shard: Shard, order_by: &[OrderBy], aggregate: &Aggregate) -> Self {
        Self {
            shard,
            order_by: order_by.to_vec(),
            read: true,
            aggregate: aggregate.clone(),
            limit: None,
        }
    }

    /// A query that should go to a replica.
    pub fn read(shard: impl Into<Shard>) -> Self {
        Self {
            shard: shard.into(),
            read: true,
            order_by: vec![],
            aggregate: Aggregate::default(),
            limit: None,
        }
    }

    /// A write query.
    pub fn write(shard: impl Into<Shard>) -> Self {
        Self {
            shard: shard.into(),
            read: false,
            order_by: vec![],
            aggregate: Aggregate::default(),
            limit: None,
        }
    }

    pub fn is_read(&self) -> bool {
        self.read
    }

    pub fn is_write(&self) -> bool {
        !self.is_read()
    }

    /// Get shard if any.
    pub fn shard(&self) -> &Shard {
        &self.shard
    }

    /// Should this query go to all shards?
    pub fn is_all_shards(&self) -> bool {
        matches!(self.shard, Shard::All)
    }

    pub fn is_multi_shard(&self) -> bool {
        matches!(self.shard, Shard::Multi(_))
    }

    pub fn order_by(&self) -> &[OrderBy] {
        &self.order_by
    }

    pub fn aggregate(&self) -> &Aggregate {
        &self.aggregate
    }

    pub fn set_shard(&mut self, shard: usize) {
        self.shard = Shard::Direct(shard);
    }

    pub fn should_buffer(&self) -> bool {
        !self.order_by().is_empty() || !self.aggregate().is_empty()
    }

    pub fn limit(&self) -> Option<Limit> {
        self.limit
    }

    pub fn with_lock(mut self, lock: bool) -> Self {
        self.read = !lock;
        self
    }

    pub fn set_read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }
}
