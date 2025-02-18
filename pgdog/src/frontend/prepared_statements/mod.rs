//! Prepared statements cache.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::net::messages::parse::Parse;

mod error;
pub use error::Error;

type Query = String;
type Name = String;

static CACHE: Lazy<PreparedStatements> = Lazy::new(PreparedStatements::default);

fn global_name(counter: usize) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Default, Debug)]
pub struct GlobalCache {
    statements: HashMap<Query, usize>,
    names: HashMap<Name, Query>, // Ideally this holds an entry to `statements`. Maybe an Arc?
    counter: usize,
}

impl GlobalCache {
    fn insert(&mut self, parse: &Parse) -> (bool, String) {
        match self.statements.entry(parse.query.clone()) {
            Entry::Occupied(entry) => (false, global_name(*entry.get())),
            Entry::Vacant(entry) => {
                self.counter += 1;
                entry.insert(self.counter);
                self.names
                    .insert(global_name(self.counter), parse.query.clone());

                (true, global_name(self.counter))
            }
        }
    }

    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.names
            .get(name)
            .map(|query| Parse::numbered(name, query))
    }
}

#[derive(Default, Clone)]
pub struct PreparedStatements {
    global: Arc<Mutex<GlobalCache>>,
    local: HashMap<String, String>,
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

    /// Register prepared statement with the global cache.
    pub fn insert(&mut self, parse: Parse) -> Parse {
        let mut guard = self.global.lock();
        let (_new, name) = guard.insert(&parse);
        self.local.insert(parse.name.clone(), name.clone());

        Parse::numbered(name, parse.query)
    }

    /// Get a parse for the given prepared statement by name.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.global.lock().parse(name)
    }

    /// Get global statement counter.
    pub fn name(&self, name: &str) -> Option<&String> {
        self.local.get(name)
    }
}
