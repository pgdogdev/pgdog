use lru::LruCache;
use std::{
    cmp::Reverse,
    time::{Duration, Instant},
};

use crate::{net::Close, util::time::deadline};
use pgdog_config::{PreparedStatementsEviction, prepared_statements::PreparedStatementsConfig};

/// Rough size of one local cache entry. Ignores the LRU node itself,
/// so it undercounts a little.
#[inline]
fn entry_mem(s: &str) -> usize {
    s.len() + std::mem::size_of::<String>() + std::mem::size_of::<LocalStatement>()
}

/// A statement info prepared on this connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LocalStatement {
    /// When this statement should be replanned
    deadline: Option<Instant>,

    /// How many times this connection has executed the statement.
    /// Relevant for [`PreparedStatementsEviction::LeastFrequentlyUsed`].
    executions: u32,
}

impl LocalStatement {
    /// Restarts the TTL, because Postgres has just planned the statement again.
    fn set_deadline(&mut self, ttl: Option<Duration>, jitter: Duration) {
        self.deadline = ttl.map(|ttl| deadline(ttl, jitter));
    }

    /// Check for expired
    ///
    /// If the check is called and deadline is not set then it's marked as expired
    /// to cover the case when the TTL was set after the statement creation
    fn expired(&self, now: Instant) -> bool {
        self.deadline.is_none_or(|deadline| deadline <= now)
    }
}

/// Where a statement stands in the local cache on this connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalStatementStatus {
    /// Not prepared on this connection.
    Missing,

    /// Reached its TTL limit, so it has to be closed and prepared again.
    Expired,

    /// Prepared and not due to be prepared again.
    Fresh,
}

/// How a client message reaches a statement in the local cache.
/// Only an `Execution` counts toward [`PreparedStatementsEviction::LeastFrequentlyUsed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Access {
    /// A Bind, or the EXECUTE an EnsurePrepared is spliced in front of.
    Execution,

    /// A Parse, a PREPARE or a Describe. Each reaches the entry without executing it.
    Lookup,
}

impl Access {
    pub(super) const fn is_execution(self) -> bool {
        matches!(self, Access::Execution)
    }
}

/// The statements prepared on one server connection, mirroring what Postgres holds.
/// Unbounded, so [`Self::ensure_capacity`] is what enforces the configured limit.
#[derive(Debug)]
pub(super) struct LocalCache {
    statements: LruCache<String, LocalStatement>,
    memory_used: usize,
}

impl LocalCache {
    pub(super) fn new() -> Self {
        Self {
            statements: LruCache::unbounded(),
            memory_used: 0,
        }
    }

    /// Status of a statement in the local cache. Promotes it either way, and counts one
    /// execution on [`Access::Execution`].
    pub(super) fn status(
        &mut self,
        name: &str,
        access: Access,
        config: &PreparedStatementsConfig,
    ) -> LocalStatementStatus {
        match self.statements.get_mut(name) {
            None => LocalStatementStatus::Missing,
            Some(statement) => {
                if access.is_execution() {
                    statement.executions = statement.executions.saturating_add(1);
                }

                if config.ttl.is_some() && statement.expired(Instant::now()) {
                    LocalStatementStatus::Expired
                } else {
                    LocalStatementStatus::Fresh
                }
            }
        }
    }

    pub(super) fn contains(&mut self, name: &str, config: &PreparedStatementsConfig) -> bool {
        !matches!(
            self.status(name, Access::Lookup, config),
            LocalStatementStatus::Missing
        )
    }

    /// Records that Postgres has the statement prepared, restarting its TTL.
    pub(super) fn prepared(&mut self, name: &str, config: &PreparedStatementsConfig) {
        let (ttl, jitter) = (config.ttl, config.ttl_jitter);

        self.statements
            .get_or_insert_mut_ref(name, || {
                // Cache is unbounded, so the insert never evicts anything to account for.
                self.memory_used += entry_mem(name);

                LocalStatement::default()
            })
            .set_deadline(ttl, jitter);
    }

    pub(super) fn executions(&self, name: &str) -> Option<u32> {
        self.statements
            .peek(name)
            .map(|statement| statement.executions)
    }

    /// Adds to a statement's execution count without promoting it, unlike [`Self::status`].
    pub(super) fn add_executions(&mut self, name: &str, executions: u32) {
        if let Some(statement) = self.statements.peek_mut(name) {
            statement.executions = statement.executions.saturating_add(executions);
        }
    }

    pub(super) fn remove(&mut self, name: &str) -> bool {
        if self.statements.pop(name).is_some() {
            self.memory_used = self.memory_used.saturating_sub(entry_mem(name));
            true
        } else {
            false
        }
    }

    pub(super) fn clear(&mut self) {
        self.statements.clear();
        self.memory_used = 0;
    }

    pub(super) fn len(&self) -> usize {
        self.statements.len()
    }

    pub(super) fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Drops every statement over `config.limit`, returning the Closes Postgres needs.
    #[must_use]
    pub(super) fn ensure_capacity(&mut self, config: &PreparedStatementsConfig) -> Vec<Close> {
        let to_evict = self.statements.len().saturating_sub(config.limit);

        match config.eviction {
            PreparedStatementsEviction::LeastRecentlyUsed => self.ensure_capacity_lru(to_evict),
            PreparedStatementsEviction::LeastFrequentlyUsed => self.ensure_capacity_lfu(to_evict),
        }
    }

    fn ensure_capacity_lru(&mut self, count: usize) -> Vec<Close> {
        let mut closed = Vec::with_capacity(count);

        for _ in 0..count {
            if let Some((name, _)) = self.statements.pop_lru() {
                self.memory_used = self.memory_used.saturating_sub(entry_mem(&name));
                closed.push(Close::named(&name));
            }
        }

        closed
    }

    fn ensure_capacity_lfu(&mut self, count: usize) -> Vec<Close> {
        if count == 0 {
            return vec![];
        }

        // iter() walks most-recently-used first, so a higher position is a less
        // recent statement, and reversing it sends the least recent of a tie first.
        let mut candidates: Vec<_> = self
            .statements
            .iter()
            .enumerate()
            .map(|(i, (name, statement))| (statement.executions, Reverse(i), name))
            .collect();

        // Splits the cache into the ones we're closing and the ones we're keeping, so
        // the sort afterwards only has the first group to order.
        candidates.select_nth_unstable_by_key(count - 1, |&(executions, i, _)| (executions, i));
        candidates.truncate(count);
        candidates.sort_unstable_by_key(|&(executions, i, _)| (executions, i));

        let closed: Vec<_> = candidates
            .into_iter()
            .map(|(_, _, name)| Close::named(name))
            .collect();

        for close in &closed {
            self.remove(close.name());
        }

        closed
    }

    #[cfg(test)]
    pub(super) fn expired(&self, name: &str) -> Option<bool> {
        self.statements
            .peek(name)
            .map(|statement| statement.expired(Instant::now()))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn config(limit: usize, eviction: PreparedStatementsEviction) -> PreparedStatementsConfig {
        PreparedStatementsConfig {
            limit,
            eviction,
            ..Default::default()
        }
    }

    fn lru(limit: usize) -> PreparedStatementsConfig {
        config(limit, PreparedStatementsEviction::LeastRecentlyUsed)
    }

    fn lfu(limit: usize) -> PreparedStatementsConfig {
        config(limit, PreparedStatementsEviction::LeastFrequentlyUsed)
    }

    #[test]
    fn ensure_capacity_evicts_in_least_recently_used_order() {
        let config = lru(3);
        let mut cache = LocalCache::new();
        for name in ["a", "b", "c", "d", "e"] {
            cache.prepared(name, &config);
        }

        let close = cache.ensure_capacity(&config);

        assert_eq!(close, [Close::named("a"), Close::named("b")]);
        assert_eq!(cache.len(), 3);
        for name in ["c", "d", "e"] {
            assert!(cache.contains(name, &config), "{name} should have survived");
        }
    }

    #[test]
    fn ensure_capacity_evicts_nothing_at_the_limit() {
        let config = lru(3);
        let mut cache = LocalCache::new();
        for name in ["a", "b", "c"] {
            cache.prepared(name, &config);
        }

        assert!(cache.ensure_capacity(&config).is_empty());
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn contains_keeps_a_statement_out_of_the_next_eviction() {
        let config = lru(2);
        let mut cache = LocalCache::new();
        for name in ["a", "b", "c"] {
            cache.prepared(name, &config);
        }

        assert!(cache.contains("a", &config));

        assert_eq!(cache.ensure_capacity(&config), [Close::named("b")]);
        assert!(cache.contains("a", &config));
    }

    #[test]
    fn prepared_keeps_a_statement_out_of_the_next_eviction() {
        let config = lru(2);
        let mut cache = LocalCache::new();
        for name in ["a", "b", "c"] {
            cache.prepared(name, &config);
        }

        cache.prepared("a", &config);

        assert_eq!(cache.ensure_capacity(&config), [Close::named("b")]);
        assert!(cache.contains("a", &config));
    }

    #[test]
    fn prepared_counts_a_repeated_name_once_in_memory_used() {
        let config = lru(usize::MAX);
        let mut cache = LocalCache::new();
        cache.prepared("a", &config);
        let once = cache.memory_used();

        cache.prepared("a", &config);

        assert_eq!(cache.memory_used(), once);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn ensure_capacity_reclaims_memory_for_evicted_statements() {
        let config = lru(1);
        let mut cache = LocalCache::new();
        for name in ["a", "b", "c"] {
            cache.prepared(name, &config);
        }

        assert_eq!(cache.ensure_capacity(&config).len(), 2);
        assert_eq!(cache.memory_used(), entry_mem("c"));
    }

    #[test]
    fn ensure_capacity_evicts_nothing_at_the_limit_under_lfu() {
        let config = lfu(3);
        let mut cache = LocalCache::new();
        for name in ["a", "b", "c"] {
            cache.prepared(name, &config);
        }

        assert!(cache.ensure_capacity(&config).is_empty());
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn ensure_capacity_reclaims_memory_for_evicted_statements_under_lfu() {
        let config = lfu(1);
        let mut cache = LocalCache::new();
        for (name, executions) in [("hot", 2), ("warm", 1), ("cold", 0)] {
            cache.prepared(name, &config);
            cache.add_executions(name, executions);
        }

        assert_eq!(cache.ensure_capacity(&config).len(), 2);
        assert_eq!(cache.memory_used(), entry_mem("hot"));
    }
}
