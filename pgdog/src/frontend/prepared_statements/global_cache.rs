use bytes::Bytes;

use crate::net::messages::{Parse, RowDescription};
use std::collections::hash_map::{Entry, HashMap};

// Format the globally unique prepared statement
// name based on the counter.
fn global_name(counter: usize) -> String {
    format!("__pgdog_{}", counter)
}

#[derive(Debug, Clone)]
pub struct Statement {
    parse: Parse,
    row_description: Option<RowDescription>,
}

impl Statement {
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
    query: Bytes,
    data_types: Bytes,
    version: usize,
}

/// Global prepared statements cache.
///
/// The cache contains two mappings:
///
/// 1. Mapping between unique prepared statement identifiers (query and result data types),
///    and the global unique prepared statement name used in all server connections.
///
/// 2. Mapping between the global unique names and Parse & RowDescription messages
///    used to prepare the statement on server connections and to decode
///    results returned by executing those statements in a multi-shard context.
///
#[derive(Default, Debug, Clone)]
pub struct GlobalCache {
    statements: HashMap<CacheKey, usize>,
    names: HashMap<String, Statement>,
    counter: usize,
    versions: usize,
}

impl GlobalCache {
    /// Record a Parse message with the global cache and return a globally unique
    /// name PgDog is using for that statement.
    ///
    /// If the statement exists, no entry is created
    /// and the global name is returned instead.
    pub fn insert(&mut self, parse: &Parse) -> (bool, String) {
        let parse_key = CacheKey {
            query: parse.query_ref(),
            data_types: parse.data_types_ref(),
            version: 0,
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
                    Statement {
                        parse,
                        row_description: None,
                    },
                );

                (true, name)
            }
        }
    }

    /// Insert a prepared statement into the global cache ignoring
    /// duplicate check.
    pub fn insert_anyway(&mut self, parse: &Parse) -> String {
        self.counter += 1;
        self.versions += 1;
        let key = CacheKey {
            query: parse.query_ref(),
            data_types: parse.data_types_ref(),
            version: self.versions,
        };

        self.statements.insert(key, self.counter);
        let name = global_name(self.counter);
        let parse = parse.rename(&name);
        self.names.insert(
            name.clone(),
            Statement {
                parse,
                row_description: None,
            },
        );

        name
    }

    /// Client sent a Describe for a prepared statement and received a RowDescription.
    /// We record the RowDescription for later use by the results decoder.
    pub fn insert_row_description(&mut self, name: &str, row_description: &RowDescription) {
        if let Some(ref mut entry) = self.names.get_mut(name) {
            if entry.row_description.is_none() {
                entry.row_description = Some(row_description.clone());
            }
        }
    }

    /// Get the query string stored in the global cache
    /// for the given globally unique prepared statement name.
    #[inline]
    pub fn query(&self, name: &str) -> Option<&str> {
        self.names.get(name).map(|s| s.query())
    }

    /// Get the Parse message for a globally unique prepared statement
    /// name.
    ///
    /// It can be used to prepare this statement on a server connection
    /// or to inspect the original query.
    pub fn parse(&self, name: &str) -> Option<Parse> {
        self.names.get(name).map(|p| p.parse.clone())
    }

    /// Get the RowDescription message for the prepared statement.
    ///
    /// It can be used to decode results received from executing the prepared
    /// statement.
    pub fn row_description(&self, name: &str) -> Option<RowDescription> {
        self.names.get(name).and_then(|p| p.row_description.clone())
    }

    /// Number of prepared statements in the local cache.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    /// True if the local cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn names(&self) -> &HashMap<String, Statement> {
        &self.names
    }
}
