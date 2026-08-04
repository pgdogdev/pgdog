//! Cache the schema per database, so we don't have to fetch
//! it for each [`crate::backend::pool::Cluster`].

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::backend::{CanonicalOids, Oids, Schema, Shard};

type Entry = Arc<Mutex<Schema>>;

/// Schema cache.
#[derive(Debug, Default, Clone)]
pub(crate) struct SchemaCache {
    // Database => shard => Schema
    cache: Arc<DashMap<(String, usize), Entry>>,
    /// The canonical mapping of type names to OID
    /// A cluster's canonical mapping of type names to OID
    canonical_oids: Arc<DashMap<String, Arc<CanonicalOids>>>,
    /// Each database, shard pair's mappings to the canonical type OIDs
    shard_oids: Arc<DashMap<(String, usize), Arc<Oids>>>,
}

impl SchemaCache {
    /// Get a schema entry from the cache or load it from
    /// the server and store it in the cache.
    ///
    /// The loading is synchronized with a mutex, so only one user
    /// can load a schema at a time, preventing a thundering herd situtation.
    pub(crate) async fn get(&self, shard: &Shard) -> Result<Schema, super::Error> {
        // This is synchronized.
        let entry = self
            .cache
            .entry((shard.identifier().database.clone(), shard.number()))
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

    pub(crate) fn canonical_oids(&self, database: &str) -> Arc<CanonicalOids> {
        Arc::clone(&self.canonical_oids.entry(database.to_owned()).or_default())
    }

    pub(crate) fn oids(&self, database: &str, shard_number: usize) -> Arc<Oids> {
        Arc::clone(
            &self
                .shard_oids
                .entry((database.to_owned(), shard_number))
                .or_insert_with(|| Oids::new(&self.canonical_oids(database))),
        )
    }
}
