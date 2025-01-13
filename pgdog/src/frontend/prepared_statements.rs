//! Prepared statements cache.

use parking_lot::Mutex;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::net::messages::parse::Parse;

/// Globally unique prepared statement identifier.
pub type StatementId = u64;

#[derive(Default)]
struct Inner {
    cache: Mutex<HashMap<Parse, StatementId>>,
    counter: AtomicU64,
}

/// Prepared statements cache.
#[derive(Default, Clone)]
pub struct Cache {
    /// Uses globally unique prepared statements.
    global: Arc<Inner>,
    /// Prepared statements prepared by the client.
    local: HashMap<String, Parse>,
}

impl Cache {
    /// New cache for a client.
    pub fn new(&self) -> Cache {
        let mut cache = self.clone();
        cache.local.clear(); // Should be empty already.
        cache
    }

    /// Save prepared statement in client cache and global cache.
    /// Global cache allows statement re-use.
    pub fn parse(&mut self, mut parse: Parse) -> Parse {
        let id = self.global.counter.fetch_add(1, Ordering::Relaxed);
        self.local.insert(parse.name.clone(), parse.clone());
        self.global.cache.lock().insert(parse.clone(), id);

        parse.name = format!("_pgdog_{}", id);
        parse
    }

    /// Remap parse to a globally unique name used on the server.
    pub fn remap(&self, name: &str) -> Option<Parse> {
        if let Some(mut parse) = self.local.get(name).cloned() {
            if let Some(id) = self.global.cache.lock().get(&parse).cloned() {
                parse.name = format!("_pgdog_{}", id);
                return Some(parse);
            }
        }

        None
    }
}
