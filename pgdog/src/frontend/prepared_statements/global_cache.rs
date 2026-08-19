use crate::{
    frontend::RewritePlan,
    net::{
        Prepare,
        messages::{Parse, RowDescription},
    },
    stats::memory::MemoryUsage,
};
use std::collections::hash_map::HashMap;

use bytes::Bytes;
use fnv::FnvHashSet as HashSet;

use super::*;

/// Global prepared statements cache.
///
/// The cache contains two mappings:
///
/// 1. Mapping between unique prepared statement identifiers (query and result data types),
///    and the global unique prepared statement name used in all server connections.
///    Statements created by SQL `PREPARE` carry a key of their own, so they are
///    never handed to a second client.
///
/// 2. Mapping between the global unique names and Parse & RowDescription messages
///    used to prepare the statement on server connections and to decode
///    results returned by executing those statements in a multi-shard context.
///
#[derive(Default, Debug, Clone)]
pub struct GlobalCache {
    statements: HashMap<CacheKey, CachedStmt>,
    names: HashMap<String, Statement>,
    unused: HashSet<Counter>,
    counter: Counter,
}

impl MemoryUsage for GlobalCache {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.statements.memory_usage()
            + self.names.memory_usage()
            + self.counter.memory_usage()
            + self.unused.capacity() * 1usize.memory_usage()
    }
}

impl GlobalCache {
    /// Record a Parse message with the global cache and return a globally unique
    /// name PgDog is using for that statement.
    ///
    /// If the statement exists, no entry is created
    /// and the global name is returned instead.
    pub(crate) fn insert(&mut self, parse: &Parse) -> (bool, String) {
        let cache_key = CacheKey::Extended {
            query: parse.query_ref(),
            data_types: parse.data_types_ref(),
        };

        if let Some(name) = self.reuse(&cache_key) {
            return (false, name);
        }

        let name = self.next_name();
        let parse = parse.renamed(&name);
        let cache_key = CacheKey::Extended {
            query: parse.query_ref(),
            data_types: parse.data_types_ref(),
        };
        let statement = Statement {
            stmt: StatementType::Parse {
                parse,
                rewrite: None,
            },
            cache_key: cache_key.clone(),
            row_description: None,
        };

        self.insert_internal(&name, cache_key, statement);

        (true, name)
    }

    /// Insert a statement prepared using the simple protocol into the global cache.
    pub(super) fn insert_prepare(
        &mut self,
        query: Bytes,
        rewrite_plan: &RewritePlan,
    ) -> (bool, Prepare) {
        let cache_key = CacheKey::Simple {
            query: query.clone(),
        };

        if let Some(name) = self.reuse(&cache_key) {
            return (
                false,
                self.prepare_and_rewrite(&name)
                    .expect("prepared to be in cache if reuse is true")
                    .0,
            );
        }

        let name = self.next_name();
        let prepare = Prepare {
            name: Bytes::from(name.clone()),
            query,
        };

        let statement = Statement {
            stmt: StatementType::Prepare {
                prepare: prepare.clone(),
                rewrite_plan: Arc::new(rewrite_plan.clone()),
            },
            row_description: None,
            cache_key: cache_key.clone(),
        };

        self.insert_internal(&name, cache_key, statement);
        (true, prepare)
    }

    /// Rewrite prepared statement in the global cache.
    pub(crate) fn rewrite(&mut self, parse: &Parse) {
        if let Some(stmt) = self.names.get_mut(parse.name()) {
            stmt.set_rewrite(parse);
        }
    }

    /// Client sent a Describe for a prepared statement and received a RowDescription.
    /// We record the RowDescription for later use by the results decoder.
    pub fn insert_row_description(&mut self, name: &str, row_description: RowDescription) {
        if let Some(entry) = self.names.get_mut(name)
            && entry.row_description.is_none()
        {
            entry.row_description = Some(row_description);
        }
    }

    /// Get the Parse message for a globally unique prepared statement
    /// name.
    ///
    /// It can be used to prepare this statement on a server connection
    /// or to inspect the original query.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.names.get(name).and_then(|p| p.parse().clone())
    }

    /// Get the [`Prepare`] message for a globally unique prepare statement name.
    pub(crate) fn prepare_and_rewrite(&self, name: &str) -> Option<(Prepare, Arc<RewritePlan>)> {
        self.names.get(name).and_then(|p| p.prepare_and_rewrite())
    }

    /// Get the rewritten Parse statement.
    ///
    /// Used for preparing this statement on a server connection.
    ///
    pub fn rewritten_parse(&self, name: &str) -> Option<Parse> {
        self.names
            .get(name)
            .and_then(|p| p.rewritten_parse().clone().or(p.parse()))
    }

    /// Returns true if this prepared statement has been
    /// rewritten by the rewrite engine.
    pub(crate) fn is_rewritten(&self, name: &str) -> bool {
        self.names
            .get(name)
            .map(|p| p.rewritten_parse().is_some())
            .unwrap_or_default()
    }

    /// Get the RowDescription message for the prepared statement.
    ///
    /// It can be used to decode results received from executing the prepared
    /// statement.
    pub(crate) fn row_description(&self, name: &str) -> Option<RowDescription> {
        self.names.get(name).and_then(|p| p.row_description.clone())
    }

    /// Number of prepared statements in the local cache.
    pub(crate) fn len(&self) -> usize {
        self.statements.len()
    }

    /// True if the local cache is empty.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Close prepared statement.
    pub(crate) fn close(&mut self, name: &str) {
        if let Some(statement) = self.names.get(name) {
            let key = statement.cache_key();

            if let Some(entry) = self.statements.get_mut(key) {
                entry.used = entry.used.saturating_sub(1);
                if entry.used == 0 {
                    self.unused.insert(entry.counter);
                }
            }
        }
    }

    /// Close unused statements until the cache is down to `capacity` entries;
    /// `0` removes everything not in use. Statements in use stay, and global
    /// names are never reused.
    pub(crate) fn close_unused(&mut self, capacity: usize) -> usize {
        let over = self.len().saturating_sub(capacity);

        // move out of unused to mutate it without borrowing the self to be able to call self.remove later
        // this helps avoid allocations to remove only part of keys from unused
        // PERF: the remove though removes once at a time in the loop, that could
        // defeat this optimization actually
        let mut unused = std::mem::take(&mut self.unused);

        let removed = unused
            .extract_if(|counter| {
                // PERF: the global_name always allocates
                // do we need to actually store this by String or
                // can we use buffer
                self.remove(&global_name(*counter));

                true
            })
            .take(over)
            .count();

        // unused will hold the remaining elements that was not extracted above
        self.unused = unused;

        removed
    }

    /// Get all prepared statements in the global cache, keyed by name.
    pub(crate) fn names(&self) -> &HashMap<String, Statement> {
        &self.names
    }

    /// Get all prepared statements in the global cache, keyed by global unique key.
    pub(crate) fn statements(&self) -> &HashMap<CacheKey, CachedStmt> {
        &self.statements
    }

    /// Remove statement from global cache.
    fn remove(&mut self, name: &str) {
        if let Some(stmt) = self.names.remove(name) {
            self.statements.remove(stmt.cache_key());
        }
    }

    fn next_name(&mut self) -> String {
        self.counter += 1;
        global_name(self.counter)
    }

    fn reuse(&mut self, cache_key: &CacheKey) -> Option<String> {
        if let Some(entry) = self.statements.get_mut(cache_key) {
            if entry.used == 0 {
                self.unused.remove(&entry.counter);
            }
            entry.used += 1;

            Some(entry.name())
        } else {
            None
        }
    }

    fn insert_internal(&mut self, name: &str, cache_key: CacheKey, statement: Statement) {
        self.statements.insert(
            cache_key,
            CachedStmt {
                counter: self.counter,
                used: 1,
            },
        );
        self.names.insert(name.to_owned(), statement);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    impl GlobalCache {
        /// Get the query string stored in the global cache
        /// for the given globally unique prepared statement name.
        pub(crate) fn query(&self, name: &str) -> Option<&str> {
            self.names.get(name).map(|s| s.query())
        }
    }

    #[test]
    fn test_close_unused_zero_keeps_in_use_and_counter() {
        let mut cache = GlobalCache::default();

        let (_, held) = cache.insert(&Parse::named("s", "SELECT 'held'"));
        let (_, released) = cache.insert(&Parse::named("s", "SELECT 'released'"));
        cache.close(&released);

        assert_eq!(cache.close_unused(0), 1, "only the released statement goes");
        assert!(cache.parse(&held).is_some(), "statements in use survive");
        assert!(cache.parse(&released).is_none());

        // A reused name could hand a server connection a different query.
        let (_, next) = cache.insert(&Parse::named("s", "SELECT 'next'"));
        assert_eq!(next, "__pgdog_3", "global names are never reused");
    }

    #[test]
    fn test_cache_key_aliases_the_stored_parse() {
        let mut cache = GlobalCache::default();
        let source = Parse::named("client_name", "SELECT $1");
        let (_, name) = cache.insert(&source);

        let stored = cache.names.get(&name).unwrap();
        let map_key = cache.statements.keys().next().unwrap();
        let owned = stored.parse().expect("parse").query_ref();

        assert_eq!(owned.as_ptr(), stored.cache_key.query_ref().as_ptr());
        assert_eq!(owned.as_ptr(), map_key.query_ref().as_ptr());
        assert_ne!(owned.as_ptr(), source.query_ref().as_ptr());
    }

    #[test]
    fn test_prep_stmt_cache_close() {
        let mut cache = GlobalCache::default();
        let parse = Parse::named("test", "SELECT $1");
        let (new, name) = cache.insert(&parse);
        assert!(new);
        assert_eq!(name, "__pgdog_1");

        for _ in 0..25 {
            let (new, name) = cache.insert(&parse);
            assert!(!new);
            assert_eq!(name, "__pgdog_1");
        }
        let stmt = cache.names.get("__pgdog_1").unwrap().clone();
        let entry = cache.statements.get(stmt.cache_key()).unwrap();

        assert_eq!(entry.used, 26);

        for _ in 0..25 {
            cache.close("__pgdog_1");
        }

        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 1);
        assert!(cache.unused.is_empty());

        cache.close("__pgdog_1");
        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 0);
        assert!(cache.unused.contains(&1)); // __pgdog_1

        // let (_, name) = cache.insert_prepare(&parse);
        // cache.close(&name);
        // assert!(cache.unused.contains(&2)); // __pgdog_2
    }

    fn used(cache: &GlobalCache, name: &str) -> usize {
        let statement = cache.names.get(name).unwrap();
        cache.statements.get(statement.cache_key()).unwrap().used
    }

    #[test]
    fn test_simple_prepared_is_never_shared() {
        let mut cache = GlobalCache::default();

        let query = Bytes::from("PREPARE __pgdog_template_name AS SELECT $1");
        let parse = Parse::named("client_stmt", "SELECT $1");

        let (_, first) = cache.insert_prepare(query.clone(), &RewritePlan::default());
        let (_, second) = cache.insert_prepare(query, &RewritePlan::default());

        assert_eq!(first, second);
        assert_eq!(cache.len(), 1);
        assert_eq!(used(&cache, first.name()), 2);
        assert_eq!(used(&cache, second.name()), 2);

        // A Parse never re-uses a SQL PREPARE statement.
        let (new, extended) = cache.insert(&parse);
        assert!(new);
        assert_ne!(extended, first.name());
        assert_ne!(extended, second.name());
        assert_eq!(cache.len(), 2);

        // A Parse re-uses another Parse.
        let (new_again, shared) = cache.insert(&parse);
        assert!(!new_again);
        assert_eq!(shared, extended);
        assert_eq!(cache.len(), 2);
        assert_eq!(used(&cache, &extended), 2);
    }

    #[test]
    fn test_remove_unused() {
        let mut cache = GlobalCache::default();
        let mut names = vec![];

        for stmt in 0..25 {
            let parse = Parse::named("__sqlx_1", format!("SELECT {}", stmt));
            let (new, name) = cache.insert(&parse);
            assert!(new);
            names.push(name);
        }

        for name in &names[0..5] {
            cache.close(name);
        }

        assert_eq!(cache.close_unused(26), 0);
        assert_eq!(cache.close_unused(21), 4);
        assert_eq!(cache.close_unused(20), 1);
        assert_eq!(cache.close_unused(19), 0);
        assert_eq!(cache.len(), 20);
    }

    #[test]
    fn test_reuse_statement_after_becomes_unused() {
        let mut cache = GlobalCache::default();
        let parse = Parse::named("test", "SELECT $1");

        let (new, name) = cache.insert(&parse);
        assert!(new);
        assert_eq!(cache.len(), 1);

        cache.close(&name);
        let stmt = cache.names.get(&name).unwrap().clone();
        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 0);
        assert!(cache.unused.contains(&1));

        let (new_again, name_again) = cache.insert(&parse);
        assert!(!new_again);
        assert_eq!(name, name_again);
        assert!(!cache.unused.contains(&1));

        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 1);
    }

    #[test]
    fn test_close_nonexistent_statement() {
        let mut cache = GlobalCache::default();
        let parse = Parse::named("test", "SELECT 1");
        cache.insert(&parse);

        cache.close("__pgdog_999");
        assert_eq!(cache.len(), 1);
        assert!(cache.unused.is_empty());
    }

    #[test]
    fn test_close_unused_with_capacity_zero() {
        let mut cache = GlobalCache::default();

        for i in 0..10 {
            let parse = Parse::named("test", format!("SELECT {}", i));
            let (_, name) = cache.insert(&parse);
            cache.close(&name);
        }

        assert_eq!(cache.len(), 10);
        assert_eq!(cache.unused.len(), 10);

        let removed = cache.close_unused(0);
        assert_eq!(removed, 10);
        assert_eq!(cache.len(), 0);
        assert!(cache.unused.is_empty());
        assert!(cache.names.is_empty());
        assert!(cache.statements.is_empty());
    }

    #[test]
    fn test_close_unused_when_nothing_unused() {
        let mut cache = GlobalCache::default();

        for i in 0..10 {
            let parse = Parse::named("test", format!("SELECT {}", i));
            cache.insert(&parse);
        }

        assert_eq!(cache.len(), 10);
        assert!(cache.unused.is_empty());

        let removed = cache.close_unused(5);
        assert_eq!(removed, 0);
        assert_eq!(cache.len(), 10);
    }

    #[test]
    fn test_close_marks_as_unused() {
        let mut cache = GlobalCache::default();
        let parse = Parse::named("test", "SELECT 1");

        let (_, name) = cache.insert(&parse);
        cache.insert(&parse);
        cache.insert(&parse);

        let stmt = cache.names.get(&name).unwrap().clone();
        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 3);

        cache.close(&name);
        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 2);
        assert!(cache.unused.is_empty());

        cache.close(&name);
        cache.close(&name);
        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 0);
        assert!(cache.unused.contains(&1));

        cache.close(&name);
        let entry = cache.statements.get(stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 0);
    }

    #[test]
    fn test_both_maps_cleaned_up_on_removal() {
        let mut cache = GlobalCache::default();
        let mut names = vec![];

        for i in 0..5 {
            let parse = Parse::named("test", format!("SELECT {}", i));
            let (_, name) = cache.insert(&parse);
            names.push(name);
        }

        assert_eq!(cache.len(), 5);
        assert_eq!(cache.statements.len(), 5);
        assert_eq!(cache.names.len(), 5);

        for name in &names {
            cache.close(name);
        }

        assert_eq!(cache.unused.len(), 5);

        cache.close_unused(0);

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.statements.len(), 0);
        assert_eq!(cache.names.len(), 0);
        assert_eq!(cache.unused.len(), 0);

        for name in &names {
            assert!(cache.parse(name).is_none());
            assert!(cache.query(name).is_none());
        }
    }

    #[test]
    fn test_complex_interleaved_operations() {
        let mut cache = GlobalCache::default();

        let parse1 = Parse::named("test", "SELECT 1");
        let parse2 = Parse::named("test", "SELECT 2");
        let parse3 = Parse::named("test", "SELECT 3");

        let (_, name1) = cache.insert(&parse1);
        let (_, name2) = cache.insert(&parse2);
        let (_, name3) = cache.insert(&parse3);

        cache.insert(&parse1);
        cache.insert(&parse1);

        assert_eq!(cache.len(), 3);

        cache.close(&name1);
        cache.close(&name2);
        cache.close(&name3);

        assert_eq!(cache.unused.len(), 2);
        assert!(cache.unused.contains(&2));
        assert!(cache.unused.contains(&3));
        assert!(!cache.unused.contains(&1));

        cache.close(&name1);
        cache.close(&name1);
        assert_eq!(cache.unused.len(), 3);
        assert!(cache.unused.contains(&1));

        cache.close_unused(2);
        assert_eq!(cache.len(), 2);

        let parse_exists = cache.parse(&name1).is_some();
        let parse_new = Parse::named("test", "SELECT 99");
        let (is_new, new_name) = cache.insert(&parse_new);
        assert!(is_new);

        cache.close(&new_name);
        assert_eq!(cache.unused.len(), 3);

        cache.close_unused(1);
        assert_eq!(cache.len(), 1);

        if parse_exists {
            assert!(cache.parse(&name1).is_some());
        }

        cache.close_unused(0);
        assert_eq!(cache.len(), 0);
        assert!(cache.statements.is_empty());
        assert!(cache.names.is_empty());
        assert!(cache.unused.is_empty());
    }
}
