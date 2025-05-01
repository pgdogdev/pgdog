//! Prepared statements cache.

use std::{collections::HashMap, sync::Arc};

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::{backend::ProtocolMessage, net::Parse};

pub mod error;
pub mod global_cache;
pub mod request;
pub mod rewrite;

pub use error::Error;
pub use global_cache::GlobalCache;
pub use request::PreparedRequest;
pub use rewrite::Rewrite;

static CACHE: Lazy<PreparedStatements> = Lazy::new(PreparedStatements::default);

#[derive(Clone, Debug)]
pub struct PreparedStatements {
    pub(super) global: Arc<Mutex<GlobalCache>>,
    pub(super) local: HashMap<String, String>,
    pub(super) enabled: bool,
}

impl Default for PreparedStatements {
    fn default() -> Self {
        Self {
            global: Arc::new(Mutex::new(GlobalCache::default())),
            local: HashMap::default(),
            enabled: true,
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
        self.local.insert(parse.name().to_owned(), name.clone());

        parse.rename(&name)
    }

    /// Insert statement into the cache bypassing duplicate checks.
    pub fn insert_anyway(&mut self, parse: Parse) -> Parse {
        let (_, name) = self.global.lock().insert(&parse);
        self.local.insert(parse.name().to_owned(), name.clone());
        parse.rename(&name)
    }

    /// Get global statement counter.
    pub fn name(&self, name: &str) -> Option<&String> {
        self.local.get(name)
    }

    /// Number of prepared statements in the local cache.
    pub fn len(&self) -> usize {
        self.local.len()
    }

    /// Is the local cache empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::Bind;

    use super::*;

    #[test]
    fn test_maybe_rewrite() {
        let mut statements = PreparedStatements::default();

        let messages = vec![
            Parse::named("__sqlx_1", "SELECT 1").into(),
            Bind::test_statement("__sqlx_1").into(),
        ];

        for message in messages {
            statements.maybe_rewrite(message).unwrap();
        }
    }
}
