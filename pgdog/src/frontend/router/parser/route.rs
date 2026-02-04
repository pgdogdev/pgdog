use std::{fmt::Display, ops::Deref};

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
    shard: ShardWithPriority,
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
    schema_changed: bool,
}

impl Display for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shard={}, role={}",
            self.shard.deref(),
            if self.read { "replica" } else { "primary" }
        )
    }
}

impl Route {
    /// Create new route for a `SELECT` query.
    pub fn select(
        shard: ShardWithPriority,
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
    pub fn read(shard: ShardWithPriority) -> Self {
        Self {
            shard,
            read: true,
            ..Default::default()
        }
    }

    /// A write query.
    pub fn write(shard: ShardWithPriority) -> Self {
        Self {
            shard,
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

    pub fn shard_with_priority(&self) -> &ShardWithPriority {
        &self.shard
    }

    /// Returns true if this query should go to all shards.
    pub fn is_all_shards(&self) -> bool {
        matches!(*self.shard, Shard::All)
    }

    /// Returns true if this query should be sent to multiple
    /// but not all shards.
    pub fn is_multi_shard(&self) -> bool {
        matches!(*self.shard, Shard::Multi(_))
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

    pub fn set_shard_mut(&mut self, shard: ShardWithPriority) {
        self.shard = shard;
    }

    pub fn with_shard(mut self, shard: ShardWithPriority) -> Self {
        self.set_shard_mut(shard);
        self
    }

    pub fn set_schema_changed(&mut self, changed: bool) {
        self.schema_changed = changed;
    }

    pub fn is_schema_changed(&self) -> bool {
        self.schema_changed
    }

    pub fn with_schema_changed(mut self, changed: bool) -> Self {
        self.schema_changed = changed;
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

    pub fn set_shard_raw_mut(&mut self, shard: ShardWithPriority) {
        self.shard = shard;
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

/// Shard source.
///
/// N.B. Ordering here matters. Don't move these around,
/// unless you're changing the algorithm.
///
/// These are ranked from least priority to highest
/// priority.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default)]
pub enum ShardSource {
    #[default]
    DefaultUnset,
    Table,
    RoundRobin(RoundRobinReason),
    SearchPath(String),
    Set,
    Comment,
    Plugin,
    Override(OverrideReason),
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum RoundRobinReason {
    PrimaryShardedTableInsert,
    Omni,
    NotExecutable,
    NoTable,
    EmptyQuery,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum OverrideReason {
    DryRun,
    ParserDisabled,
    Transaction,
    OnlyOneShard,
    RewriteUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default)]
pub struct ShardWithPriority {
    source: ShardSource,
    shard: Shard,
}

impl ShardWithPriority {
    /// Create new shard with comment-level priority.
    pub fn new_comment(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Comment,
        }
    }

    pub fn new_plugin(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Plugin,
        }
    }

    /// Create new shard with table-level priority.
    pub fn new_table(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Table,
        }
    }

    /// Create new shard with highest priority.
    pub fn new_override_parser_disabled(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Override(OverrideReason::ParserDisabled),
        }
    }

    pub fn new_override_rewrite_update(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Override(OverrideReason::RewriteUpdate),
        }
    }

    pub fn new_override_dry_run(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Override(OverrideReason::DryRun),
        }
    }

    pub fn new_override_transaction(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Override(OverrideReason::Transaction),
        }
    }

    pub fn new_override_only_one_shard(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Override(OverrideReason::OnlyOneShard),
        }
    }

    pub fn new_default_unset(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::DefaultUnset,
        }
    }

    pub fn new_rr_omni(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::RoundRobin(RoundRobinReason::Omni),
        }
    }

    pub fn new_rr_not_executable(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::RoundRobin(RoundRobinReason::NotExecutable),
        }
    }

    pub fn new_rr_primary_insert(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::RoundRobin(RoundRobinReason::PrimaryShardedTableInsert),
        }
    }

    pub fn new_rr_no_table(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::RoundRobin(RoundRobinReason::NoTable),
        }
    }

    pub fn new_rr_empty_query(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::RoundRobin(RoundRobinReason::EmptyQuery),
        }
    }

    /// New SET-based routing.
    pub fn new_set(shard: Shard) -> Self {
        Self {
            shard,
            source: ShardSource::Set,
        }
    }

    /// New search_path-based shard.
    pub fn new_search_path(shard: Shard, schema: &str) -> Self {
        Self {
            shard,
            source: ShardSource::SearchPath(schema.to_string()),
        }
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> &ShardSource {
        &self.source
    }
}

impl Deref for ShardWithPriority {
    type Target = Shard;

    fn deref(&self) -> &Self::Target {
        &self.shard
    }
}

/// Ordered collection of set shards.
#[derive(Default, Debug, Clone)]
pub struct ShardsWithPriority {
    max: Option<ShardWithPriority>,
}

impl ShardsWithPriority {
    /// Get currently computed shard.
    pub(crate) fn shard(&self) -> ShardWithPriority {
        lazy_static! {
            static ref DEFAULT_SHARD: ShardWithPriority = ShardWithPriority {
                shard: Shard::All,
                source: ShardSource::DefaultUnset,
            };
        }

        self.peek().cloned().unwrap_or(DEFAULT_SHARD.clone())
    }

    pub(crate) fn push(&mut self, shard: ShardWithPriority) {
        if let Some(ref max) = self.max {
            if max < &shard {
                self.max = Some(shard);
            }
        } else {
            self.max = Some(shard);
        }
    }

    pub(crate) fn peek(&self) -> Option<&ShardWithPriority> {
        self.max.as_ref()
    }

    /// Schema-path based routing priority is used.
    pub(crate) fn is_search_path(&self) -> bool {
        self.peek()
            .map(|shard| matches!(shard.source, ShardSource::SearchPath(_)))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_shard_ord() {
        assert!(Shard::Direct(0) < Shard::All);
        assert!(Shard::Multi(vec![]) < Shard::All);
    }

    #[test]
    fn test_source_ord() {
        assert!(ShardSource::Table < ShardSource::RoundRobin(RoundRobinReason::NotExecutable));
        assert!(ShardSource::Table < ShardSource::SearchPath(String::new()));
        assert!(ShardSource::SearchPath(String::new()) < ShardSource::Set);
        assert!(ShardSource::Set < ShardSource::Comment);
        assert!(ShardSource::Comment < ShardSource::Override(OverrideReason::OnlyOneShard));
    }

    #[test]
    fn test_shard_with_priority_ord() {
        let shard = Shard::Direct(0);

        assert!(
            ShardWithPriority::new_table(shard.clone())
                < ShardWithPriority::new_rr_omni(shard.clone())
        );
        assert!(
            ShardWithPriority::new_table(shard.clone())
                < ShardWithPriority::new_search_path(shard.clone(), "schema")
        );
        assert!(
            ShardWithPriority::new_search_path(shard.clone(), "schema")
                < ShardWithPriority::new_set(shard.clone())
        );
        assert!(
            ShardWithPriority::new_set(shard.clone())
                < ShardWithPriority::new_comment(shard.clone())
        );
        assert!(
            ShardWithPriority::new_comment(shard.clone())
                < ShardWithPriority::new_override_dry_run(shard.clone())
        );
    }

    #[test]
    fn test_comment_override_set() {
        let mut shards = ShardsWithPriority::default();

        shards.push(ShardWithPriority::new_set(Shard::Direct(1)));
        assert_eq!(shards.shard().deref(), &Shard::Direct(1));

        shards.push(ShardWithPriority::new_comment(Shard::Direct(2)));
        assert_eq!(shards.shard().deref(), &Shard::Direct(2));

        let mut shards = ShardsWithPriority::default();

        shards.push(ShardWithPriority::new_comment(Shard::Direct(3)));
        assert_eq!(shards.shard().deref(), &Shard::Direct(3));

        shards.push(ShardWithPriority::new_set(Shard::Direct(4)));
        assert_eq!(shards.shard().deref(), &Shard::Direct(3));
    }
}
