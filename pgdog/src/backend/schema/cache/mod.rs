//! Cache the schema per database, so we don't have to fetch
//! it for each [`crate::backend::pool::Cluster`].

use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::backend::{CanonicalOids, Oids, Schema, Shard};

type Entry = Arc<Mutex<Option<Schema>>>;

#[cfg(test)]
mod test;

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
    /// Start a new cache generation, retaining entries untouched by this DDL.
    pub(crate) fn with_invalidated_shards(
        &self,
        targets: &HashMap<String, HashSet<usize>>,
    ) -> Self {
        let changed = |db: &str, shard: usize| {
            targets
                .get(db)
                .is_some_and(|shards| shards.contains(&shard))
        };
        // Every shard's type mapping depends on the canonical OIDs from shard 0.
        let canonical_changed = |db: &str| changed(db, 0);

        Self {
            cache: Arc::new(
                self.cache
                    .iter()
                    .filter(|entry| !changed(&entry.key().0, entry.key().1))
                    .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
                    .collect(),
            ),
            canonical_oids: Arc::new(
                self.canonical_oids
                    .iter()
                    .filter(|entry| !canonical_changed(entry.key()))
                    .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
                    .collect(),
            ),
            shard_oids: Arc::new(
                self.shard_oids
                    .iter()
                    .filter(|entry| {
                        !canonical_changed(&entry.key().0)
                            && !changed(&entry.key().0, entry.key().1)
                    })
                    .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
                    .collect(),
            ),
        }
    }

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

        if let Some(schema) = guard.as_ref() {
            return Ok(schema.clone());
        }

        let schema = shard.fetch_schema().await?;

        *guard = Some(schema.clone());

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
