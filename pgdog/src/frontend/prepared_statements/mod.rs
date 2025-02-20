//! Prepared statements cache.

use std::{collections::HashMap, sync::Arc};

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::net::messages::{Message, Parse, Protocol};

pub mod error;
pub mod global_cache;
pub mod rewrite;

pub use error::Error;
pub use global_cache::GlobalCache;
pub use rewrite::Rewrite;

static CACHE: Lazy<PreparedStatements> = Lazy::new(PreparedStatements::default);

#[derive(Default, Clone, Debug)]
pub struct PreparedStatements {
    pub(super) global: Arc<Mutex<GlobalCache>>,
    pub(super) local: HashMap<String, String>,
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
        rewrite.rewrite(message)
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
}
