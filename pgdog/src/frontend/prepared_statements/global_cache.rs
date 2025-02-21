use crate::net::messages::Parse;
use std::collections::hash_map::{Entry, HashMap};

fn global_name(counter: usize) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Default, Debug)]
pub struct GlobalCache {
    statements: HashMap<String, usize>,
    names: HashMap<String, String>, // Ideally this holds an entry to `statements`. Maybe an Arc?
    counter: usize,
}

impl GlobalCache {
    pub(super) fn insert(&mut self, parse: &Parse) -> (bool, String) {
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

    /// Get query stored in the global cache.
    #[inline]
    pub fn query(&self, name: &str) -> Option<&String> {
        self.names.get(name)
    }

    /// Construct a Parse message from a query stored in the global cache.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.query(name).map(|query| Parse::named(name, query))
    }

    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
