//! Cache the schema per database, so we don't have to fetch
//! it for each [`crate::backend::pool::Cluster`].

use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::backend::{Schema, Shard};

type Entry = Arc<Mutex<Schema>>;

static CACHE: Lazy<SchemaCache> = Lazy::new(SchemaCache::default);

/// Schema cache.
#[derive(Debug, Default)]
pub(crate) struct SchemaCache {
    // Database => shard => Schema
    cache: DashMap<String, DashMap<usize, Entry>>,
}

impl SchemaCache {
    /// Get a schema entry from the cache or load it from
    /// the server and store it in the cache.
    ///
    /// The loading is synchronized with a mutex, so only one user
    /// can load a schema at a time, preventing a thundering herd situtation.
    ///
    pub(crate) async fn get(&self, shard: &Shard) -> Result<Schema, super::Error> {
        // This is synchronized.
        let entry = self
            .cache
            .entry(shard.identifier().database.clone())
            .or_default()
            .entry(shard.number())
            .or_default()
            .clone();

        // This is syncrhonized too,
        // so only one shard/user can fetch the schema at a time.
        let mut guard = entry.lock().await;

        if guard.is_loaded() {
            return Ok(guard.clone());
        }

        let schema = shard.fetch_schema().await?;

        *guard = schema.clone();

        Ok(schema)
    }

    /// Remove all entries from the schema cache.
    pub(crate) fn clear(&self) {
        self.cache.clear();
    }

    /// Get a reference the the global schema cache.
    pub(crate) fn global() -> &'static SchemaCache {
        &CACHE
    }
}
