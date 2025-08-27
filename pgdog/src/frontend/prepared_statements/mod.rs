//! Prepared statements cache.

use std::{collections::HashMap, sync::Arc};

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::{
    net::{Parse, ProtocolMessage},
    stats::memory::MemoryUsage,
};

pub mod error;
pub mod global_cache;
pub mod rewrite;

pub use error::Error;
pub use global_cache::GlobalCache;

pub use rewrite::Rewrite;

static CACHE: Lazy<PreparedStatements> = Lazy::new(PreparedStatements::default);

#[derive(Clone, Debug)]
pub struct PreparedStatements {
    pub(super) global: Arc<Mutex<GlobalCache>>,
    pub(super) local: HashMap<String, String>,
    pub(super) enabled: bool,
    pub(super) capacity: usize,
    pub(super) memory_used: usize,
}

impl MemoryUsage for PreparedStatements {
    #[inline]
    fn memory_usage(&self) -> usize {
        self.local.memory_usage()
            + self.enabled.memory_usage()
            + self.capacity.memory_usage()
            + std::mem::size_of::<Arc<Mutex<GlobalCache>>>()
    }
}

impl Default for PreparedStatements {
    fn default() -> Self {
        Self {
            global: Arc::new(Mutex::new(GlobalCache::default())),
            local: HashMap::default(),
            enabled: true,
            capacity: usize::MAX,
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
    pub fn global() -> Arc<Mutex<GlobalCache>> {
        Self::new().global.clone()
    }

    /// Maybe rewrite message.
    pub fn maybe_rewrite(&mut self, message: ProtocolMessage) -> Result<ProtocolMessage, Error> {
        let mut rewrite = Rewrite::new(self);
        let message = rewrite.rewrite(message)?;
        Ok(message)
    }

    /// Register prepared statement with the global cache.
    pub fn insert(&mut self, parse: Parse) -> Parse {
        let (_new, name) = { self.global.lock().insert(&parse) };
        let existed = self.local.insert(parse.name().to_owned(), name.clone());
        self.memory_used = self.memory_usage();

        // Client prepared it again because it got an error the first time.
        // We can check if this is a new statement first, but this is an error
        // condition which happens very infrequently, so we optimize for the happy path.
        if existed.is_some() {
            {
                self.global.lock().decrement(&name);
            }
        }

        parse.rename_fast(&name)
    }

    /// Insert statement into the cache bypassing duplicate checks.
    pub fn insert_anyway(&mut self, parse: Parse) -> Parse {
        let name = self.global.lock().insert_anyway(&parse);
        self.local.insert(parse.name().to_owned(), name.clone());
        self.memory_used = self.memory_usage();
        parse.rename_fast(&name)
    }

    /// Get global statement counter.
    pub fn name(&self, name: &str) -> Option<&String> {
        self.local.get(name)
    }

    /// Number of prepared statements in the local cache.
    pub fn len_local(&self) -> usize {
        self.local.len()
    }

    /// Is the local cache empty?
    pub fn is_empty(&self) -> bool {
        self.len_local() == 0
    }

    /// Remove prepared statement from local cache.
    pub fn close(&mut self, name: &str) {
        if let Some(global_name) = self.local.remove(name) {
            self.global.lock().close(&global_name, self.capacity);
            self.memory_used = self.memory_usage();
        }
    }

    /// Close all prepared statements on this client.
    pub fn close_all(&mut self) {
        if !self.local.is_empty() {
            let mut global = self.global.lock();

            for global_name in self.local.values() {
                global.close(global_name, self.capacity);
            }
        }

        self.local.clear();
        self.memory_used = self.memory_usage();
    }

    /// How much memory is used, approx.
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::Bind;

    use super::*;

    #[test]
    fn test_maybe_rewrite() {
        let mut statements = PreparedStatements::default();
        statements.capacity = 0;

        let messages = vec![
            Parse::named("__sqlx_1", "SELECT 1").into(),
            Bind::new_statement("__sqlx_1").into(),
        ];

        for message in messages {
            statements.maybe_rewrite(message).unwrap();
        }

        assert_eq!(statements.local.len(), 1);
        assert_eq!(statements.global.lock().names().len(), 1);

        statements.close_all();

        assert!(statements.local.is_empty());
        assert!(statements.global.lock().names().is_empty());

        let messages = vec![
            Parse::named("__sqlx_1", "SELECT 1").into(),
            Bind::new_statement("__sqlx_1").into(),
        ];

        for message in messages {
            statements.maybe_rewrite(message).unwrap();
        }

        assert_eq!(statements.local.len(), 1);
        assert_eq!(statements.global.lock().names().len(), 1);

        statements.close("__sqlx_1");

        assert!(statements.local.is_empty());
        assert!(statements.global.lock().names().is_empty());
    }

    #[test]
    fn test_counted_only_once_per_client() {
        let mut statements = PreparedStatements::default();

        let messages = vec![
            Parse::named("__sqlx_1", "SELECT 1").into(),
            Bind::new_statement("__sqlx_1").into(),
        ];

        for _ in 0..25 {
            for message in messages.clone() {
                statements.maybe_rewrite(message).unwrap();
            }
        }

        assert_eq!(
            statements
                .global
                .lock()
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
                .lock()
                .statements()
                .iter()
                .next()
                .unwrap()
                .1
                .used,
            0
        );
    }
}
