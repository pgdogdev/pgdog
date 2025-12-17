use std::{
    collections::BinaryHeap,
    fmt::Display,
    ops::{Deref, DerefMut},
};

use lazy_static::lazy_static;

use super::{
    explain_trace::ExplainTrace, rewrite::statement::aggregate::AggregateRewritePlan, Aggregate,
    DistinctBy, FunctionBehavior, Limit, LockingBehavior, OrderBy,
};

/// The shard destination for a statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Default)]
pub enum Shard {
    /// Direct-to-shard number.
    Direct(usize),
    /// Multiple shards, enumerated.
    Multi(Vec<usize>),
    /// All shards.
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
    /// Returns true if this is an all-shard query.
    pub fn is_all(&self) -> bool {
        matches!(self, Shard::All)
    }

    /// Create new direct-to-shard mapping.
    pub fn new_direct(shard: usize) -> Self {
        Self::Direct(shard)
    }

    /// Returns true if this is a direct-to-shard mapping.
    pub fn is_direct(&self) -> bool {
        matches!(self, Self::Direct(_))
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

impl From<usize> for Shard {
    fn from(value: usize) -> Self {
        Shard::Direct(value)
    }
}

impl From<Vec<usize>> for Shard {
    fn from(value: Vec<usize>) -> Self {
        Shard::Multi(value)
    }
}

/// Path a query should take and any transformations
/// that should be applied to the response.
#[derive(Debug, Clone, Default, PartialEq, derive_builder::Builder)]
pub struct Route {
    shard: Shard,
    read: bool,
    order_by: Vec<OrderBy>,
    aggregate: Aggregate,
    limit: Limit,
    lock_session: bool,
    distinct: Option<DistinctBy>,
    maintenance: bool,
    rewrite_plan: AggregateRewritePlan,
    rewritten_sql: Option<String>,
    explain: Option<ExplainTrace>,
    rollback_savepoint: bool,
    search_path_driven: bool,
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
    /// Create new route for a `SELECT` query.
    pub fn select(
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
    pub fn read(shard: impl Into<Shard>) -> Self {
        Self {
            shard: shard.into(),
            read: true,
            ..Default::default()
        }
    }

    /// A write query.
    pub fn write(shard: impl Into<Shard>) -> Self {
        Self {
            shard: shard.into(),
            ..Default::default()
        }
    }

    /// Returns true if this is a query that
    /// can be sent to a replica.
    pub fn is_read(&self) -> bool {
        self.read
    }

    /// Returns true if this query can only be sent
    /// to a primary.
    pub fn is_write(&self) -> bool {
        !self.is_read()
    }

    /// Get shard if any.
    pub fn shard(&self) -> &Shard {
        &self.shard
    }

    /// Returns true if this query should go to all shards.
    pub fn is_all_shards(&self) -> bool {
        matches!(self.shard, Shard::All)
    }

    /// Returns true if this query should be sent to multiple
    /// but not all shards.
    pub fn is_multi_shard(&self) -> bool {
        matches!(self.shard, Shard::Multi(_))
    }

    /// Returns true if this query should be sent to
    /// more than one shard.
    pub fn is_cross_shard(&self) -> bool {
        self.is_all_shards() || self.is_multi_shard()
    }

    pub fn order_by(&self) -> &[OrderBy] {
        &self.order_by
    }

    pub fn aggregate(&self) -> &Aggregate {
        &self.aggregate
    }

    pub fn aggregate_mut(&mut self) -> &mut Aggregate {
        &mut self.aggregate
    }

    pub fn set_shard_mut(&mut self, shard: impl Into<Shard>) {
        self.shard = shard.into();
    }

    pub fn with_shard(mut self, shard: impl Into<Shard>) -> Self {
        self.set_shard_mut(shard);
        self
    }

    pub fn set_search_path_driven_mut(&mut self, schema_driven: bool) {
        self.search_path_driven = schema_driven;
    }

    pub fn is_search_path_driven(&self) -> bool {
        self.search_path_driven
    }

    pub fn is_maintenance(&self) -> bool {
        self.maintenance
    }

    pub fn set_shard_raw_mut(&mut self, shard: &Shard) {
        self.shard = shard.clone();
    }

    pub fn should_buffer(&self) -> bool {
        !self.order_by().is_empty() || !self.aggregate().is_empty() || self.distinct().is_some()
    }

    pub fn limit(&self) -> &Limit {
        &self.limit
    }

    pub fn with_read(mut self, read: bool) -> Self {
        self.set_read(read);
        self
    }

    pub fn set_read(&mut self, read: bool) {
        self.read = read;
    }

    pub fn explain(&self) -> Option<&ExplainTrace> {
        self.explain.as_ref()
    }

    pub fn set_explain(&mut self, trace: ExplainTrace) {
        self.explain = Some(trace);
    }

    pub fn take_explain(&mut self) -> Option<ExplainTrace> {
        self.explain.take()
    }

    pub fn with_rollback_savepoint(mut self, rollback: bool) -> Self {
        self.rollback_savepoint = rollback;
        self
    }

    pub fn rollback_savepoint(&self) -> bool {
        self.rollback_savepoint
    }

    pub fn with_write(mut self, write: FunctionBehavior) -> Self {
        self.set_write(write);
        self
    }

    pub fn set_write(&mut self, write: FunctionBehavior) {
        let FunctionBehavior {
            writes,
            locking_behavior,
        } = write;
        self.read = !writes;
        self.lock_session = matches!(locking_behavior, LockingBehavior::Lock);
    }

    pub fn is_lock_session(&self) -> bool {
        self.lock_session
    }

    pub fn distinct(&self) -> &Option<DistinctBy> {
        &self.distinct
    }

    pub fn should_2pc(&self) -> bool {
        self.is_cross_shard() && self.is_write() && !self.is_maintenance()
    }

    pub fn aggregate_rewrite_plan(&self) -> &AggregateRewritePlan {
        &self.rewrite_plan
    }

    pub fn with_aggregate_rewrite_plan_mut(&mut self, plan: AggregateRewritePlan) {
        self.rewrite_plan = plan;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum ShardSource {
    SearchPath(String),
    Table,
    Set,
    Comment,
    Override,
    RoundRobin,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord)]
pub struct ShardWithPriority {
    priority: u8,
    shard: Shard,
    source: ShardSource,
}

impl ShardWithPriority {
    /// Create new shard with comment-level priority.
    pub fn new_comment(shard: Shard) -> Self {
        Self {
            shard,
            priority: 0,
            source: ShardSource::Comment,
        }
    }

    /// Create new shard with table-level priority.
    pub fn new_table(shard: Shard) -> Self {
        Self {
            shard,
            priority: 5,
            source: ShardSource::Table,
        }
    }

    /// Create new shard with highest priority.
    pub fn new_override(shard: Shard) -> Self {
        Self {
            shard,
            priority: 0,
            source: ShardSource::Override,
        }
    }

    /// New round-robin shard path.
    pub fn new_rr(shard: Shard) -> Self {
        Self {
            shard,
            priority: 6,
            source: ShardSource::RoundRobin,
        }
    }

    /// New omni/sticky routing.
    pub fn new_omni(shard: Shard) -> Self {
        Self::new_rr(shard)
    }

    /// New SET-based routing.
    pub fn new_set(shard: Shard) -> Self {
        Self {
            shard,
            priority: 1,
            source: ShardSource::Set,
        }
    }

    /// New search_path-based shard.
    pub fn new_search_path(shard: Shard, schema: &str) -> Self {
        Self {
            shard,
            priority: 4,
            source: ShardSource::SearchPath(schema.to_string()),
        }
    }
}

impl Deref for ShardWithPriority {
    type Target = Shard;

    fn deref(&self) -> &Self::Target {
        &self.shard
    }
}

impl PartialOrd for ShardWithPriority {
    /// Order by priority in ascending order.
    ///
    /// 0 = highest priority (first)
    /// 9 = lowest priority (last)
    ///
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.priority.partial_cmp(&self.priority)
    }
}

/// Ordered collection of set shards.
#[derive(Default, Debug, Clone)]
pub struct ShardsWithPriority {
    shards: BinaryHeap<ShardWithPriority>,
}

impl Deref for ShardsWithPriority {
    type Target = BinaryHeap<ShardWithPriority>;

    fn deref(&self) -> &Self::Target {
        &self.shards
    }
}

impl DerefMut for ShardsWithPriority {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shards
    }
}

impl ShardsWithPriority {
    /// Get currently computed shard.
    pub fn shard(&self) -> &Shard {
        lazy_static! {
            static ref DEFAULT_SHARD: ShardWithPriority = ShardWithPriority {
                shard: Shard::All,
                priority: 0,
                source: ShardSource::Override,
            };
        }

        self.shards.peek().unwrap_or(&DEFAULT_SHARD)
    }

    /// Schema-path based routing priority is used.
    pub fn is_search_path(&self) -> bool {
        self.shards
            .peek()
            .map(|shard| matches!(shard.source, ShardSource::SearchPath(_)))
            .unwrap_or_default()
    }
}
