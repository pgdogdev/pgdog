//! AST cache.

use once_cell::sync::Lazy;
use pg_query::*;
use std::collections::HashMap;

use parking_lot::Mutex;
use std::sync::Arc;

static CACHE: Lazy<Cache> = Lazy::new(Cache::default);

/// AST cache.
#[derive(Default, Clone, Debug)]
pub struct Cache {
    queries: Arc<Mutex<HashMap<String, Arc<ParseResult>>>>,
}

impl Cache {
    /// Parse a statement by either getting it from cache
    /// or using pg_query parser.
    ///
    /// N.B. There is a race here that allows multiple threads to
    /// parse the same query. That's better imo than locking the data structure
    /// while we parse the query.
    pub fn parse(&mut self, query: &str) -> Result<Arc<ParseResult>> {
        if let Some(ast) = self.queries.lock().get(query) {
            return Ok(ast.clone());
        }
        let ast = Arc::new(parse(query)?);
        self.queries.lock().insert(query.to_owned(), ast.clone());
        Ok(ast)
    }

    /// Get global cache instance.
    pub fn get() -> Self {
        CACHE.clone()
    }
}
