use crate::net::messages::Parse;
use std::collections::hash_map::{Entry, HashMap};

fn global_name(counter: usize) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Debug, Clone)]
struct StoredParse {
    parse: Parse,
}

impl StoredParse {
    pub fn query(&self) -> &String {
        &self.parse.query
    }
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
struct ParseKey {
    query: String,
    data_types: Vec<i32>,
}

#[derive(Default, Debug)]
pub struct GlobalCache {
    statements: HashMap<ParseKey, usize>,
    names: HashMap<String, StoredParse>, // Ideally this holds an entry to `statements`. Maybe an Arc?
    counter: usize,
}

impl GlobalCache {
    pub(super) fn insert(&mut self, parse: &Parse) -> (bool, String) {
        let parse_key = ParseKey {
            query: parse.query.clone(),
            data_types: parse.data_types.clone(),
        };
        match self.statements.entry(parse_key) {
            Entry::Occupied(entry) => (false, global_name(*entry.get())),
            Entry::Vacant(entry) => {
                self.counter += 1;
                entry.insert(self.counter);
                let name = global_name(self.counter);
                let mut parse = parse.clone();
                parse.name = name.clone();
                self.names.insert(name.clone(), StoredParse { parse });

                (true, name)
            }
        }
    }

    /// Get query stored in the global cache.
    #[inline]
    pub fn query(&self, name: &str) -> Option<&String> {
        self.names.get(name).map(|s| s.query())
    }

    /// Construct a Parse message from a query stored in the global cache.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.names.get(name).map(|p| p.parse.clone())
    }

    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
