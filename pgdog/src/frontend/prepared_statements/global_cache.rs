use bytes::Bytes;

use crate::{
    frontend::router::parser::RewritePlan,
    net::messages::{Parse, RowDescription},
    stats::memory::MemoryUsage,
};
use std::{collections::hash_map::HashMap, str::from_utf8};

// Format the globally unique prepared statement
// name based on the counter.
fn global_name(counter: usize) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Debug, Clone)]
pub struct Statement {
    parse: Parse,
    row_description: Option<RowDescription>,
    #[allow(dead_code)]
    version: usize,
    rewrite_plan: Option<RewritePlan>,
    cache_key: CacheKey,
    evict_on_close: bool,
}

impl MemoryUsage for Statement {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.parse.len()
            + if let Some(ref row_description) = self.row_description {
                row_description.memory_usage()
            } else {
                0
            }
            + self.cache_key.memory_usage()
            + self.evict_on_close.memory_usage()
    }
}

impl Statement {
    pub fn query(&self) -> &str {
        self.parse.query()
    }

    fn cache_key(&self) -> CacheKey {
        self.cache_key.clone()
    }
}

/// Prepared statements cache key.
///
/// If these match, it's effectively the same statement.
/// If they don't, e.g. client sent the same query but
/// with different data types, we can't re-use it and
/// need to plan a new one.
///
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct CacheKey {
    pub query: Bytes,
    pub data_types: Bytes,
    pub version: usize,
}

impl MemoryUsage for CacheKey {
    #[inline]
    fn memory_usage(&self) -> usize {
        // Bytes refer to memory allocated by someone else.
        std::mem::size_of::<Bytes>() * 2 + self.version.memory_usage()
    }
}

impl CacheKey {
    pub fn query(&self) -> Result<&str, crate::net::Error> {
        // Postgres string.
        Ok(from_utf8(&self.query[0..self.query.len() - 1])?)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CachedStmt {
    pub counter: usize,
    pub used: usize,
}

impl MemoryUsage for CachedStmt {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.counter.memory_usage() + self.used.memory_usage()
    }
}

impl CachedStmt {
    pub fn name(&self) -> String {
        global_name(self.counter)
    }
}

/// Global prepared statements cache.
///
/// The cache contains two mappings:
///
/// 1. Mapping between unique prepared statement identifiers (query and result data types),
///    and the global unique prepared statement name used in all server connections.
///
/// 2. Mapping between the global unique names and Parse & RowDescription messages
///    used to prepare the statement on server connections and to decode
///    results returned by executing those statements in a multi-shard context.
///
#[derive(Default, Debug, Clone)]
pub struct GlobalCache {
    statements: HashMap<CacheKey, CachedStmt>,
    names: HashMap<String, Statement>,
    counter: usize,
    versions: usize,
}

impl MemoryUsage for GlobalCache {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.statements.memory_usage()
            + self.names.memory_usage()
            + self.counter.memory_usage()
            + self.versions.memory_usage()
    }
}

impl GlobalCache {
    /// Record a Parse message with the global cache and return a globally unique
    /// name PgDog is using for that statement.
    ///
    /// If the statement exists, no entry is created
    /// and the global name is returned instead.
    pub fn insert(&mut self, parse: &Parse) -> (bool, String) {
        let parse_key = CacheKey {
            query: parse.query_ref(),
            data_types: parse.data_types_ref(),
            version: 0,
        };

        if let Some(entry) = self.statements.get_mut(&parse_key) {
            entry.used += 1;
            (false, global_name(entry.counter))
        } else {
            self.counter += 1;
            let name = global_name(self.counter);
            let parse = parse.rename(&name);

            let cache_key = CacheKey {
                query: parse.query_ref(),
                data_types: parse.data_types_ref(),
                version: 0,
            };

            self.statements.insert(
                cache_key.clone(),
                CachedStmt {
                    counter: self.counter,
                    used: 1,
                },
            );

            self.names.insert(
                name.clone(),
                Statement {
                    parse,
                    row_description: None,
                    version: 0,
                    rewrite_plan: None,
                    cache_key,
                    evict_on_close: false,
                },
            );

            (true, name)
        }
    }

    /// Insert a prepared statement into the global cache ignoring
    /// duplicate check.
    pub fn insert_anyway(&mut self, parse: &Parse) -> String {
        self.counter += 1;
        self.versions += 1;

        let name = global_name(self.counter);
        let parse = parse.rename(&name);

        let key = CacheKey {
            query: parse.query_ref(),
            data_types: parse.data_types_ref(),
            version: self.versions,
        };

        self.statements.insert(
            key.clone(),
            CachedStmt {
                counter: self.counter,
                used: 1,
            },
        );

        self.names.insert(
            name.clone(),
            Statement {
                parse,
                row_description: None,
                version: self.versions,
                rewrite_plan: None,
                cache_key: key,
                evict_on_close: false,
            },
        );

        name
    }

    /// Client sent a Describe for a prepared statement and received a RowDescription.
    /// We record the RowDescription for later use by the results decoder.
    pub fn insert_row_description(&mut self, name: &str, row_description: &RowDescription) {
        if let Some(ref mut entry) = self.names.get_mut(name) {
            if entry.row_description.is_none() {
                entry.row_description = Some(row_description.clone());
            }
        }
    }

    pub fn update_and_set_rewrite_plan(
        &mut self,
        name: &str,
        sql: &str,
        plan: RewritePlan,
    ) -> bool {
        if let Some(statement) = self.names.get_mut(name) {
            statement.parse.set_query(sql);
            if !plan.is_noop() {
                statement.evict_on_close = !plan.helpers().is_empty();
                statement.rewrite_plan = Some(plan);
            } else {
                statement.evict_on_close = false;
                statement.rewrite_plan = None;
            }
            true
        } else {
            false
        }
    }

    pub fn rewrite_plan(&self, name: &str) -> Option<RewritePlan> {
        self.names.get(name).and_then(|s| s.rewrite_plan.clone())
    }

    #[cfg(test)]
    pub fn reset(&mut self) {
        self.statements.clear();
        self.names.clear();
        self.counter = 0;
        self.versions = 0;
    }

    /// Get the query string stored in the global cache
    /// for the given globally unique prepared statement name.
    #[inline]
    pub fn query(&self, name: &str) -> Option<&str> {
        self.names.get(name).map(|s| s.query())
    }

    /// Get the Parse message for a globally unique prepared statement
    /// name.
    ///
    /// It can be used to prepare this statement on a server connection
    /// or to inspect the original query.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.names.get(name).map(|p| p.parse.clone())
    }

    /// Get the RowDescription message for the prepared statement.
    ///
    /// It can be used to decode results received from executing the prepared
    /// statement.
    pub fn row_description(&self, name: &str) -> Option<RowDescription> {
        self.names.get(name).and_then(|p| p.row_description.clone())
    }

    /// Number of prepared statements in the local cache.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    /// True if the local cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Close prepared statement.
    pub fn close(&mut self, name: &str, capacity: usize) -> bool {
        if let Some(statement) = self.names.get(name) {
            let key = statement.cache_key();
            let mut used_remaining = None;

            if let Some(entry) = self.statements.get_mut(&key) {
                entry.used = entry.used.saturating_sub(1);
                used_remaining = Some(entry.used);
                if entry.used == 0 && (statement.evict_on_close || self.len() > capacity) {
                    self.remove(name);
                    return true;
                }
            }

            return used_remaining.map(|u| u > 0).unwrap_or(false);
        }

        false
    }

    /// Close all unused statements exceeding capacity.
    pub fn close_unused(&mut self, capacity: usize) -> usize {
        if capacity == 0 {
            let removed = self.statements.len();
            self.statements.clear();
            self.names.clear();
            return removed;
        }

        let mut remove = self.statements.len() as i64 - capacity as i64;
        let mut to_remove = vec![];
        for stmt in self.statements.values() {
            if remove <= 0 {
                break;
            }

            if stmt.used == 0 {
                to_remove.push(stmt.name());
                remove -= 1;
            }
        }

        for name in &to_remove {
            self.remove(name);
        }

        to_remove.len()
    }

    /// Remove statement from global cache.
    fn remove(&mut self, name: &str) {
        if let Some(stmt) = self.names.remove(name) {
            self.statements.remove(&stmt.cache_key());
        }
    }

    /// Decrement usage of prepared statement without removing it.
    pub fn decrement(&mut self, name: &str) {
        if let Some(stmt) = self.names.get(name) {
            if let Some(stmt) = self.statements.get_mut(&stmt.cache_key()) {
                stmt.used = stmt.used.saturating_sub(1);
            }
        }
    }

    /// Get all prepared statements by name.
    pub fn names(&self) -> &HashMap<String, Statement> {
        &self.names
    }

    pub fn statements(&self) -> &HashMap<CacheKey, CachedStmt> {
        &self.statements
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
        let entry = cache.statements.get(&stmt.cache_key()).unwrap();

        assert_eq!(entry.used, 26);

        for _ in 0..25 {
            cache.close("__pgdog_1", 0);
        }

        let entry = cache.statements.get(&stmt.cache_key()).unwrap();
        assert_eq!(entry.used, 1);

        cache.close("__pgdog_1", 0);
        assert!(cache.statements.is_empty());
        assert!(cache.names.is_empty());

        let name = cache.insert_anyway(&parse);
        cache.close(&name, 0);

        assert!(cache.names.is_empty());
        assert!(cache.statements.is_empty());
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

        assert_eq!(cache.close_unused(0), 0);

        for name in &names[0..5] {
            assert!(!cache.close(name, 25)); // Won't close because
                                             // capacity is enough to keep unused around.
        }

        assert_eq!(cache.close_unused(26), 0);
        assert_eq!(cache.close_unused(21), 4);
        assert_eq!(cache.close_unused(20), 1);
        assert_eq!(cache.close_unused(19), 0);
        assert_eq!(cache.len(), 20);
    }

    #[test]
    fn test_close_unused_zero_clears_all_entries() {
        let mut cache = GlobalCache::default();

        for idx in 0..5 {
            let parse = Parse::named("test", format!("SELECT {}", idx));
            let (_is_new, _name) = cache.insert(&parse);
        }

        assert!(cache.len() > 0);

        let removed = cache.close_unused(0);
        assert_eq!(removed, 5);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_update_query_reuses_cache_key() {
        let mut cache = GlobalCache::default();
        let parse = Parse::named("__sqlx_1", "SELECT 1");
        let (is_new, name) = cache.insert(&parse);
        assert!(is_new);

        assert!(cache.update_and_set_rewrite_plan(
            &name,
            "SELECT 1 ORDER BY 1",
            RewritePlan::default()
        ));

        let key = cache
            .statements()
            .keys()
            .next()
            .expect("statement key missing");
        assert_eq!(key.query().unwrap(), "SELECT 1");
        assert_eq!(cache.query(&name).unwrap(), "SELECT 1 ORDER BY 1");

        let parse_again = Parse::named("__sqlx_2", "SELECT 1");
        let (is_new_again, reused_name) = cache.insert(&parse_again);
        assert!(!is_new_again);
        assert_eq!(reused_name, name);
        assert_eq!(cache.len(), 1);
    }
}
