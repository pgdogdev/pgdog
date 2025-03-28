//! Prepared statements cache.

use std::{collections::HashMap, sync::Arc};

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::net::messages::{Message, Parse, Protocol};

pub mod error;
pub mod global_cache;
pub mod request;
pub mod rewrite;

pub use error::Error;
pub use global_cache::GlobalCache;
pub use request::PreparedRequest;
pub use rewrite::Rewrite;

static CACHE: Lazy<PreparedStatements> = Lazy::new(PreparedStatements::default);

#[derive(Clone, Debug, Default)]
pub struct PreparedStatements {
    pub(super) global: Arc<Mutex<GlobalCache>>,
    pub(super) local: HashMap<String, String>,
    pub(super) requests: Vec<PreparedRequest>,
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
    pub fn maybe_rewrite(&mut self, message: impl Protocol) -> Result<Message, Error> {
        let mut rewrite = Rewrite::new(self);
        let message = rewrite.rewrite(message)?;
        let requests = rewrite.requests();
        for request in requests {
            if !self.exists(&request) {
                self.requests.push(request);
            }
        }
        Ok(message)
    }

    /// Register prepared statement with the global cache.
    fn insert(&mut self, parse: Parse) -> Parse {
        let mut guard = self.global.lock();
        let (_new, name) = guard.insert(&parse);
        self.local.insert(parse.name.clone(), name.clone());

        Parse::named(name, parse.query)
    }

    /// Get global statement counter.
    fn name(&self, name: &str) -> Option<&String> {
        self.local.get(name)
    }

    /// Number of prepared stamenets in the local cache.
    pub fn len(&self) -> usize {
        self.local.len()
    }

    /// Is the local cache empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get requests.
    pub fn requests(&mut self) -> Vec<PreparedRequest> {
        std::mem::take(&mut self.requests).into_iter().collect()
    }

    pub fn exists(&self, request: &PreparedRequest) -> bool {
        for r in self.requests.iter() {
            if r.name() == request.name() && r.is_prepare() && request.is_prepare() {
                return true;
            }
        }

        false
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
            Parse::named("__sqlx_1", "SELECT 1").message().unwrap(),
            Bind {
                statement: "__sqlx_1".into(),
                ..Default::default()
            }
            .message()
            .unwrap(),
        ];

        for message in messages {
            statements.maybe_rewrite(message).unwrap();
        }

        let requests = statements.requests();
        assert_eq!(requests.len(), 2);
        let request = requests.first().unwrap();
        assert_eq!(request.name(), "__pgdog_1");
        assert!(request.is_new());
        let request = requests.last().unwrap();
        assert_eq!(request.name(), "__pgdog_1");
        assert!(!request.is_new());
    }
}
