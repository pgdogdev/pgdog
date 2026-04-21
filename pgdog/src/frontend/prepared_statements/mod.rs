//! Prepared statements cache.

use std::{collections::HashMap, sync::Arc, time::Duration};

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::{spawn, time::sleep};
use tracing::debug;

use crate::{
    config::{config, PreparedStatements as PreparedStatementsLevel},
    net::{Parse, ProtocolMessage},
};

pub mod error;
pub mod global_cache;
pub mod rewrite;

pub use error::Error;
pub use global_cache::GlobalCache;
pub use rewrite::Rewrite;

static CACHE: Lazy<PreparedStatements> = Lazy::new(PreparedStatements::default);

/// Approximate memory used by a String.
#[inline]
fn str_mem(s: &str) -> usize {
    s.len() + std::mem::size_of::<String>()
}

#[derive(Clone, Debug)]
pub struct PreparedStatements {
    pub(super) global: Arc<RwLock<GlobalCache>>,
    pub(super) local: HashMap<String, String>,
    pub(super) level: PreparedStatementsLevel,
    pub(super) memory_used: usize,
}

impl Default for PreparedStatements {
    fn default() -> Self {
        Self {
            global: Arc::new(RwLock::new(GlobalCache::default())),
            local: HashMap::default(),
            level: PreparedStatementsLevel::Extended,
            memory_used: 0,
        }
    }
}

impl PreparedStatements {
    /// New shared prepared statements cache.
    pub fn new() -> Self {
        CACHE.clone()
    }

    /// Get global cache.
    pub fn global() -> Arc<RwLock<GlobalCache>> {
        Self::new().global.clone()
    }

    /// Maybe rewrite message.
    pub fn maybe_rewrite(&mut self, message: &mut ProtocolMessage) -> Result<(), Error> {
        let mut rewrite = Rewrite::new(self);
        rewrite.rewrite(message)?;
        Ok(())
    }

    /// Register prepared statement with the global cache.
    pub fn insert(&mut self, parse: &mut Parse) {
        let (_new, name) = { self.global.write().insert(parse) };
        let key = parse.name();
        let existed = self.local.insert(key.to_owned(), name.clone());

        // Client prepared it again because it got an error the first time.
        // We can check if this is a new statement first, but this is an error
        // condition which happens very infrequently, so we optimize for the happy path.
        if let Some(old_value) = existed {
            // Key already existed, only value changed.
            self.memory_used = self.memory_used.saturating_sub(str_mem(&old_value));
            self.memory_used += str_mem(&name);
            self.global.write().decrement(&old_value);
        } else {
            // New entry.
            self.memory_used += str_mem(key) + str_mem(&name);
        }

        parse.rename_fast(&name)
    }

    /// Insert statement into the cache bypassing duplicate checks.
    pub fn insert_anyway(&mut self, parse: &mut Parse) {
        let name = { self.global.write().insert_anyway(parse) };
        let key = parse.name();
        let existed = self.local.insert(key.to_owned(), name.clone());

        if let Some(old_value) = existed {
            // Key already existed, only value changed.
            self.memory_used = self.memory_used.saturating_sub(str_mem(&old_value));
            self.memory_used += str_mem(&name);
        } else {
            // New entry.
            self.memory_used += str_mem(key) + str_mem(&name);
        }

        parse.rename_fast(&name)
    }

    /// Get global statement counter.
    pub fn name(&self, name: &str) -> Option<&String> {
        self.local.get(name)
    }

    /// Get globally-prepared statement by local name.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.local
            .get(name)
            .and_then(|name| self.global.read().parse(name))
    }

    /// Number of prepared statements in the local cache.
    pub fn len_local(&self) -> usize {
        self.local.len()
    }

    /// Current prepared statements compatibility level.
    #[cfg(test)]
    pub fn level(&self) -> PreparedStatementsLevel {
        self.level
    }

    /// Is the local cache empty?
    pub fn is_empty(&self) -> bool {
        self.len_local() == 0
    }

    /// Remove prepared statement from local cache.
    pub fn close(&mut self, name: &str) {
        if let Some(global_name) = self.local.remove(name) {
            self.global.write().close(&global_name);
            self.memory_used = self
                .memory_used
                .saturating_sub(str_mem(name) + str_mem(&global_name));
        }
    }

    /// Close all prepared statements on this client.
    pub fn close_all(&mut self) {
        if !self.local.is_empty() {
            let mut global = self.global.write();

            for global_name in self.local.values() {
                global.close(global_name);
            }
        }

        self.local.clear();
        self.memory_used = 0;
    }

    /// How much memory is used, approx.
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Set the prepared statements level.
    pub fn set_level(&mut self, level: PreparedStatementsLevel) {
        self.level = level;
    }
}

/// Run prepared statements maintenance task
/// every second.
pub fn start_maintenance() {
    spawn(async move {
        debug!("prepared statements cache maintenance started");
        loop {
            sleep(Duration::from_secs(1)).await;
            run_maintenance();
        }
    });
}

/// Check prepared statements cache for overflows
/// and remove any unused statements exceeding the limit.
pub fn run_maintenance() {
    let capacity = config().config.general.prepared_statements_limit;
    PreparedStatements::global().write().close_unused(capacity);
}

#[cfg(test)]
mod test {
    use crate::net::messages::Bind;

    use super::*;

    #[test]
    fn test_maybe_rewrite() {
        let mut statements = PreparedStatements::default();

        let mut messages = vec![
            ProtocolMessage::from(Parse::named("__sqlx_1", "SELECT 1")),
            ProtocolMessage::from(Bind::new_statement("__sqlx_1")),
        ];

        for message in &mut messages {
            statements.maybe_rewrite(message).unwrap();
        }

        assert_eq!(statements.local.len(), 1);
        assert_eq!(statements.global.read().names().len(), 1);

        statements.close_all();

        assert!(statements.local.is_empty());

        let mut messages = vec![
            ProtocolMessage::from(Parse::named("__sqlx_1", "SELECT 1")),
            ProtocolMessage::from(Bind::new_statement("__sqlx_1")),
        ];

        for message in &mut messages {
            statements.maybe_rewrite(message).unwrap();
        }

        assert_eq!(statements.local.len(), 1);
        assert_eq!(statements.global.read().names().len(), 1);

        statements.close("__sqlx_1");

        assert!(statements.local.is_empty());
    }

    #[test]
    fn test_counted_only_once_per_client() {
        let mut statements = PreparedStatements::default();

        for _ in 0..25 {
            let mut messages = vec![
                ProtocolMessage::from(Parse::named("__sqlx_1", "SELECT 1")),
                ProtocolMessage::from(Bind::new_statement("__sqlx_1")),
            ];

            for message in &mut messages {
                statements.maybe_rewrite(message).unwrap();
            }
        }

        assert_eq!(
            statements
                .global
                .read()
                .statements()
                .iter()
                .next()
                .unwrap()
                .1
                .used,
            1
        );

        statements.close("__sqlx_1");

        assert_eq!(
            statements
                .global
                .read()
                .statements()
                .iter()
                .next()
                .unwrap()
                .1
                .used,
            0
        );
    }

    /// Regression test: anonymous statements with different query texts
    /// must decrement the OLD global entry, not the new one.
    /// Previously, the new entry was immediately set to used=0 (evictable)
    /// while the old entry leaked at used=1 forever.
    #[test]
    fn test_anonymous_different_queries_decrement_old() {
        let mut statements = PreparedStatements::default();

        // First anonymous Parse: "" → __pgdog_1, used: 1
        let mut parse1 = ProtocolMessage::from(Parse::new_anonymous("SELECT 1"));
        statements.maybe_rewrite(&mut parse1).unwrap();

        let global = statements.global.read();
        let first = global.statements().values().next().unwrap();
        assert_eq!(first.used, 1);
        let first_name = first.name();
        drop(global);

        // Second anonymous Parse with DIFFERENT query: "" → __pgdog_2
        // This replaces the local "" mapping.
        let mut parse2 = ProtocolMessage::from(Parse::new_anonymous("SELECT 2"));
        statements.maybe_rewrite(&mut parse2).unwrap();

        let global = statements.global.read();
        assert_eq!(global.statements().len(), 2);

        for stmt in global.statements().values() {
            if stmt.name() == first_name {
                // Old entry: should be decremented to 0 (no longer referenced).
                assert_eq!(stmt.used, 0, "old entry should be decremented");
            } else {
                // New entry: should stay at 1 (actively referenced).
                assert_eq!(stmt.used, 1, "new entry should remain at used=1");
            }
        }
        drop(global);

        // Third anonymous Parse with yet another query.
        let mut parse3 = ProtocolMessage::from(Parse::new_anonymous("SELECT 3"));
        statements.maybe_rewrite(&mut parse3).unwrap();

        let global = statements.global.read();
        assert_eq!(global.statements().len(), 3);

        // Exactly one entry should have used=1 (the latest).
        let active = global.statements().values().filter(|s| s.used == 1).count();
        assert_eq!(active, 1, "exactly one statement should be active");

        // The other two should have used=0.
        let unused = global.statements().values().filter(|s| s.used == 0).count();
        assert_eq!(unused, 2, "old statements should be unused");
    }
}
