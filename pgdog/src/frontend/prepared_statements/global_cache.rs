use crate::net::messages::{Parse, RowDescription};
use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

fn global_name(counter: usize) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Debug, Clone)]
struct StoredParse {
    parse: Parse,
    row_description: Option<RowDescription>,
}

impl StoredParse {
    pub fn query(&self) -> &str {
        self.parse.query()
    }
}

/// Prepared statements cache key.
///
/// If these match, it's effectively the same statement.
/// If they don't, e.g. client sent the same query but
/// with different data types, we can't re-use it and
/// need to plan a new one.
///
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
struct CacheKey {
    query: Arc<String>,
    data_types: Arc<Vec<i32>>,
}

#[derive(Default, Debug)]
pub struct GlobalCache {
    statements: HashMap<CacheKey, usize>,
    names: HashMap<String, StoredParse>, // Ideally this holds an entry to `statements`. Maybe an Arc?
    counter: usize,
}

impl GlobalCache {
    pub(super) fn insert(&mut self, parse: &Parse) -> (bool, String) {
        let parse_key = CacheKey {
            query: parse.query_ref(),
            data_types: parse.data_types_ref(),
        };
        match self.statements.entry(parse_key) {
            Entry::Occupied(entry) => (false, global_name(*entry.get())),
            Entry::Vacant(entry) => {
                self.counter += 1;
                entry.insert(self.counter);
                let name = global_name(self.counter);
                let parse = parse.rename(&name);
                self.names.insert(
                    name.clone(),
                    StoredParse {
                        parse,
                        row_description: None,
                    },
                );

                (true, name)
            }
        }
    }

    pub fn describe(&mut self, name: &str, row_description: &RowDescription) {
        if let Some(ref mut entry) = self.names.get_mut(name) {
            if entry.row_description.is_none() {
                entry.row_description = Some(row_description.clone());
            }
        }
    }

    /// Get query stored in the global cache.
    #[inline]
    pub fn query(&self, name: &str) -> Option<&str> {
        self.names.get(name).map(|s| s.query())
    }

    /// Construct a Parse message from a query stored in the global cache.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.names.get(name).map(|p| p.parse.clone())
    }

    pub fn row_description(&self, name: &str) -> Option<RowDescription> {
        self.names.get(name).and_then(|p| p.row_description.clone())
    }

    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
