//! Prepared statements cache.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

use crate::{
    config::PreparedStatements as PreparedStatementsLevel,
    frontend::RewritePlan,
    net::{Parse, Prepare, ProtocolMessage},
};

mod cache_key;
mod cached_statement;
pub mod error;
pub mod global_cache;
mod maintenance;
mod prelude;
pub mod rewrite;
pub mod statement;

pub(crate) use cache_key::CacheKey;
pub(crate) use cached_statement::global_name;
pub(crate) use cached_statement::{CachedStmt, Counter};
pub(crate) use error::Error;
pub(crate) use global_cache::GlobalCache;
// Maintenance tasks are spawned in main.rs.
pub use maintenance::*;
pub(crate) use rewrite::Rewrite;
pub(crate) use statement::{Statement, StatementType};

static CACHE: Lazy<PreparedStatements> = Lazy::new(PreparedStatements::default);

#[derive(Clone, Debug)]
pub struct PreparedStatements {
    pub(super) global: Arc<RwLock<GlobalCache>>,
    // mapping the client statement name -> __pgdog__ name from global cache
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
    /// New shared prepared statements cache instance.
    ///
    /// Has access to the global cache singleton.
    ///
    pub(crate) fn new() -> Self {
        CACHE.clone()
    }

    /// Get global prepared statements cache singleton.
    pub(crate) fn global() -> Arc<RwLock<GlobalCache>> {
        Self::new().global.clone()
    }

    /// Rewrite extended protocol messages to use global names. This allows multiple
    /// clients to re-use the same statement prepared on a Postgres server.
    ///
    /// # Arguments
    ///
    /// * `message`: Any protocol message. Unsupported messages are not rewritten.
    ///
    pub(crate) fn maybe_rewrite(&mut self, message: &mut ProtocolMessage) -> Result<(), Error> {
        let mut rewrite = Rewrite::new(self);
        rewrite.rewrite(message)?;
        Ok(())
    }

    /// Register prepared statement with the global cache and rewrite it
    /// to use the globally unique name.
    ///
    /// # Arguments
    ///
    /// * `parse`: [`Parse`] message. It will be renamed in-place.
    ///
    pub(super) fn insert(&mut self, parse: &mut Parse) {
        let (_new, name) = { self.global.write().insert(parse) };
        let key = parse.name();

        self.insert_internal(key, &name);

        parse.rename(&name)
    }

    fn insert_internal(&mut self, local: &str, global: &str) {
        let existed = self.local.insert(local.to_owned(), global.to_owned());

        if let Some(old_value) = existed {
            // Key already existed, only value changed.
            self.memory_used = self.memory_used.saturating_sub(str_mem(&old_value));
            self.memory_used += str_mem(local);
            self.global.write().close(&old_value);
        } else {
            // New entry.
            self.memory_used += str_mem(local) + str_mem(global);
        }
    }

    /// Insert PREPARE statement into the cache.
    ///
    /// # Arguments
    ///
    /// - `parse`: [`Parse`] message, with the prepared statement named by the client.
    ///
    /// # Return
    ///
    /// Nothing, but the message is renamed to a unique, global name.
    ///
    pub(crate) fn insert_prepare(
        &mut self,
        name: &str,
        query: Bytes,
        rewrite_plan: &RewritePlan,
    ) -> Prepare {
        let (_new, prepare) = { self.global.write().insert_prepare(query, rewrite_plan) };

        self.insert_internal(name, prepare.name());

        prepare
    }

    /// Get the global unique name for a prepared statement
    /// using the name the client gave us as key.
    pub fn name(&self, name: &str) -> Option<&String> {
        self.local.get(name)
    }

    /// Get a globally unique [`Prepare`] message using the client name as key.
    pub(crate) fn prepare_and_unique_ids(&self, name: &str) -> Option<(Prepare, u16)> {
        self.local
            .get(name)
            .and_then(|name| self.global.read().prepare_and_unique_ids(name))
    }

    /// Number of prepared statements in the client's cache.
    pub(crate) fn num_statements(&self) -> usize {
        self.local.len()
    }

    /// Remove prepared statement from client's cache.
    ///
    /// # Arguments
    ///
    /// * `name`: Name of the prepared statement according to the client.
    ///
    pub(crate) fn close(&mut self, name: &str) {
        if let Some(global_name) = self.local.remove(name) {
            self.global.write().close(&global_name);
            self.memory_used = self
                .memory_used
                .saturating_sub(str_mem(name) + str_mem(&global_name));
        }
    }

    /// Close all prepared statements on this client.
    ///
    /// This only happens when the client disconnects. This will update
    /// the global usage counters of all of client's prepared statements.
    pub(super) fn close_all(&mut self) {
        if !self.local.is_empty() {
            let mut global = self.global.write();

            for global_name in self.local.values() {
                global.close(global_name);
            }
        }

        self.local.clear();
        self.memory_used = 0;
    }

    /// How much memory is used, approximately, by the prepared statements cache
    /// for this client.
    pub(crate) fn memory_used(&self) -> usize {
        self.memory_used
    }

    /// Set the prepared statements level.
    pub(crate) fn set_level(&mut self, level: PreparedStatementsLevel) {
        self.level = level;
    }
}

/// Approximate memory used by a String.
#[inline]
fn str_mem(s: &str) -> usize {
    s.len() + std::mem::size_of::<String>()
}

#[cfg(test)]
mod test {

    impl PreparedStatements {
        /// Current prepared statements compatibility level.
        pub(crate) fn level(&self) -> PreparedStatementsLevel {
            self.level
        }
    }

    use crate::backend::Server;
    use crate::backend::server::test::{execute_prepared, prepared_in_postgres, test_server};
    use crate::net::messages::Bind;

    use super::*;

    async fn prepared_names(server: &mut Server) -> Vec<String> {
        prepared_in_postgres(server)
            .await
            .into_iter()
            .map(|statement| statement.name)
            .collect()
    }

    #[tokio::test]
    async fn test_close_unused_does_not_reuse_names() {
        let mut client = PreparedStatements::new();

        let mut first = Parse::named("client_a", "SELECT $1::bigint");
        client.insert(&mut first);
        let first_name = first.name();

        let mut server = test_server().await;
        assert_eq!(execute_prepared(&mut server, first_name, b"1").await, [1]);
        assert_eq!(prepared_names(&mut server).await, [first_name]);

        client.close("client_a");
        PreparedStatements::global().write().close_unused(0);
        assert!(PreparedStatements::global().read().is_empty());

        assert_eq!(prepared_names(&mut server).await, [first_name]);

        let mut second = Parse::named("client_b", "SELECT $1::bigint + 100");
        client.insert(&mut second);
        let second_name = second.name();
        assert_ne!(second_name, first_name);

        assert_eq!(
            execute_prepared(&mut server, second_name, b"1").await,
            [101]
        );
    }

    #[tokio::test]
    async fn test_close_unused_keeps_statements_clients_still_hold() {
        let mut client = PreparedStatements::new();

        let mut parse = Parse::named("client_a", "SELECT $1::bigint");
        client.insert(&mut parse);
        let name = parse.name();

        let mut warm = test_server().await;
        assert_eq!(execute_prepared(&mut warm, name, b"1").await, [1]);
        assert_eq!(prepared_names(&mut warm).await, [name]);

        PreparedStatements::global().write().close_unused(0);

        assert_eq!(execute_prepared(&mut warm, name, b"1").await, [1]);

        let mut cold = test_server().await;
        assert!(prepared_names(&mut cold).await.is_empty());
        assert_eq!(execute_prepared(&mut cold, name, b"1").await, [1]);
        assert_eq!(prepared_names(&mut cold).await, [name]);
    }

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

        let live = global
            .statements()
            .values()
            .find(|stmt| stmt.used == 1)
            .unwrap()
            .name();
        drop(global);

        // Both replaced statements are evicted.
        assert_eq!(statements.global.write().close_unused(0), 2);

        // The statement the client still holds survives.
        let global = statements.global.read();
        assert_eq!(global.len(), 1);
        assert!(global.names().contains_key(&live));
    }
}
