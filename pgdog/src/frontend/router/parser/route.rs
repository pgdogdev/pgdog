use std::fmt::Display;

use super::{Aggregate, DistinctBy, FunctionBehavior, Limit, LockingBehavior, OrderBy};

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Default)]
pub(crate) enum Shard {
    Direct(usize),
    Multi(Vec<usize>),
    #[default]
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
    pub(crate) fn all(&self) -> bool {
        matches!(self, Shard::All)
    }

    pub(crate) fn direct(shard: usize) -> Self {
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

/// Path a query should take and any transformations
/// that should be applied along the way.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Route {
    shard: Shard,
    read: bool,
    order_by: Vec<OrderBy>,
    aggregate: Aggregate,
    limit: Limit,
    lock_session: bool,
    distinct: Option<DistinctBy>,
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

impl Route {
    /// SELECT query.
    pub(crate) fn select(
        shard: Shard,
        order_by: Vec<OrderBy>,
        aggregate: Aggregate,
        limit: Limit,
        distinct: Option<DistinctBy>,
    ) -> Self {
        Self {
            shard,
            order_by,
            read: true,
            aggregate,
            limit,
            distinct,
            ..Default::default()
        }
    }

    /// A query that should go to a replica.
    pub(crate) fn read(shard: impl Into<Shard>) -> Self {
        Self {
            shard: shard.into(),
            read: true,
            ..Default::default()
        }
    }

    /// A write query.
    pub(crate) fn write(shard: impl Into<Shard>) -> Self {
        Self {
            shard: shard.into(),
            ..Default::default()
        }
    }

    pub(crate) fn is_read(&self) -> bool {
        self.read
    }

    pub(crate) fn is_write(&self) -> bool {
        !self.is_read()
    }

    /// Get shard if any.
    pub(crate) fn shard(&self) -> &Shard {
        &self.shard
    }

    /// Should this query go to all shards?
    pub(crate) fn is_all_shards(&self) -> bool {
        matches!(self.shard, Shard::All)
    }

    pub(crate) fn is_multi_shard(&self) -> bool {
        matches!(self.shard, Shard::Multi(_))
    }

    pub(crate) fn is_cross_shard(&self) -> bool {
        self.is_all_shards() || self.is_multi_shard()
    }

    pub(crate) fn order_by(&self) -> &[OrderBy] {
        &self.order_by
    }

    pub(crate) fn aggregate(&self) -> &Aggregate {
        &self.aggregate
    }

    pub(crate) fn set_shard_mut(&mut self, shard: usize) {
        self.shard = Shard::Direct(shard);
    }

    pub(crate) fn set_shard(mut self, shard: usize) -> Self {
        self.set_shard_mut(shard);
        self
    }

    pub(crate) fn set_shard_raw_mut(&mut self, shard: &Shard) {
        self.shard = shard.clone();
    }

    pub(crate) fn should_buffer(&self) -> bool {
        !self.order_by().is_empty() || !self.aggregate().is_empty() || self.distinct().is_some()
    }

    pub(crate) fn limit(&self) -> &Limit {
        &self.limit
    }

    pub(crate) fn set_read(mut self, read: bool) -> Self {
        self.set_read_mut(read);
        self
    }

    pub(crate) fn set_read_mut(&mut self, read: bool) {
        self.read = read;
    }

    pub(crate) fn set_write(mut self, write: FunctionBehavior) -> Self {
        self.set_write_mut(write);
        self
    }

    pub(crate) fn set_write_mut(&mut self, write: FunctionBehavior) {
        let FunctionBehavior {
            writes,
            locking_behavior,
        } = write;
        self.read = !writes;
        self.lock_session = matches!(locking_behavior, LockingBehavior::Lock);
    }

    pub(crate) fn set_lock_session(mut self) -> Self {
        self.lock_session = true;
        self
    }

    pub(crate) fn lock_session(&self) -> bool {
        self.lock_session
    }

    pub(crate) fn distinct(&self) -> &Option<DistinctBy> {
        &self.distinct
    }
}
