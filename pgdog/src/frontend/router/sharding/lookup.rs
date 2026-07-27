//! Sharding key lookup maps.
//!
//! Tables configured with a `lookup` translate sharding key values through
//! an in-memory copy of the lookup table, loaded whole with the configured
//! two-column query. The lookup table must have a row for every value that
//! routes through it: a value absent from a freshly loaded map is an error,
//! not a fallback, so a miss can always be verified by reloading instead of
//! requiring cross-instance invalidation.
//!
//! Writes to the lookup table observed by the router invalidate the map
//! when they complete; the map is reloaded before any statement that needs
//! a translation routes.
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use crate::backend::ShardingSchema;

static CACHE: Lazy<LookupCache> = Lazy::new(LookupCache::new);

/// A value absent from a map loaded within this window is authoritatively
/// missing. Misses against older maps trigger a verifying reload first,
/// so this bounds the reload rate under sustained traffic for unknown
/// values without letting them route unverified.
const MISS_VERIFY_COOLDOWN: Duration = Duration::from_secs(1);

/// A lookup recorded during routing, satisfied by the query engine
/// before the statement executes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingLookup {
    /// Lookup table name.
    pub lookup: String,
    /// Query that loads the lookup table into memory.
    pub query: String,
    /// Sharding key value that wasn't in the map. `None` only ensures
    /// the map is loaded, e.g. before COPY rows are sharded.
    pub value: Option<String>,
}

/// Result of translating a sharding key value through a lookup map.
#[derive(Debug, Clone, PartialEq)]
pub enum MapLookup {
    /// The map isn't loaded: never loaded, or invalidated by a write.
    NotLoaded,
    /// The value translates.
    Hit(Arc<str>),
    /// The value isn't in a freshly loaded map: the lookup table has no
    /// row for it.
    Missing,
    /// The value isn't in the map, but the map is old enough that the
    /// row may have been added since it was loaded, e.g. through another
    /// PgDog instance. Reload to verify before treating it as missing.
    Stale,
}

/// Result of claiming a lookup table load.
pub enum ResolveClaim {
    /// The caller loads the map. Dropping the guard releases the claim
    /// and wakes waiters; insert the map before dropping it.
    Run(ResolveGuard),
    /// Another client is loading the map; await the notification, then
    /// read the map.
    Wait(Arc<Notify>),
}

/// Claim on a lookup table while its map is loading, so concurrent
/// misses don't all load it. Released on drop, waking clients waiting
/// on the load.
pub struct ResolveGuard {
    cache: &'static LookupCache,
    query: String,
}

impl Drop for ResolveGuard {
    fn drop(&mut self) {
        let notify = self.cache.in_flight.lock().remove(&self.query);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
}

/// A loaded lookup table.
#[derive(Debug)]
struct TableMap {
    /// Lookup table the entries were loaded from, for invalidation.
    lookup: String,
    /// Sharding key value to translated value.
    entries: HashMap<String, Arc<str>>,
    /// When the map was loaded, for miss verification.
    loaded_at: Instant,
}

/// Invalidation generation of a lookup table. A load carries the
/// generation it started from; [`LookupCache::insert_map`] rejects the
/// load if the generation moved, so a load that straddles an
/// invalidation (or a config reload) is re-read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupGeneration {
    /// Bumped when the whole cache is cleared on config reload, so a
    /// load that straddles a reload can't install under a lookup name
    /// whose generation counter restarted at zero.
    epoch: u64,
    /// Bumped every time the lookup table is invalidated.
    counter: u64,
}

/// Loaded maps and per-table invalidation generations, kept under one
/// lock so a load can't install a map concurrently with the
/// invalidation that should have dropped it.
#[derive(Debug, Default)]
struct Maps {
    /// Loaded lookup tables, keyed by the query that loads them.
    maps: HashMap<String, TableMap>,
    /// Invalidation counter per lookup table.
    generations: HashMap<String, u64>,
    /// Config reload counter.
    epoch: u64,
    /// The (lookup, query) pairs the maps were loaded under, to detect
    /// lookup configuration changes on config reload.
    config: Vec<(String, String)>,
}

/// Sharding key lookup maps. Reads happen on every routing decision
/// for lookup-configured tables; writes only on loads and
/// invalidations, hence the read-write lock.
#[derive(Debug)]
pub struct LookupCache {
    maps: RwLock<Maps>,
    /// Lookup tables currently loading, keyed by query, with the
    /// notification waking clients waiting on the load.
    in_flight: Mutex<HashMap<String, Arc<Notify>>>,
    /// Writes to lookup tables are rejected while set, so a warmed
    /// map stays exact, e.g. for the duration of resharding.
    writes_paused: AtomicBool,
}

impl LookupCache {
    /// Create new cache. Should only be done once at pooler startup.
    fn new() -> Self {
        Self {
            maps: RwLock::new(Maps::default()),
            in_flight: Mutex::new(HashMap::new()),
            writes_paused: AtomicBool::new(false),
        }
    }

    /// Get global cache instance.
    pub fn get() -> &'static Self {
        &CACHE
    }

    /// Translate a sharding key value through the map loaded by the
    /// given query.
    pub fn lookup(&self, query: &str, value: &str) -> MapLookup {
        let maps = self.maps.read();

        let Some(map) = maps.maps.get(query) else {
            return MapLookup::NotLoaded;
        };

        match map.entries.get(value) {
            Some(translated) => MapLookup::Hit(translated.clone()),
            None => {
                if map.loaded_at.elapsed() <= MISS_VERIFY_COOLDOWN {
                    MapLookup::Missing
                } else {
                    MapLookup::Stale
                }
            }
        }
    }

    /// The map loaded by the given query is in memory.
    pub fn loaded(&self, query: &str) -> bool {
        self.maps.read().maps.contains_key(query)
    }

    /// Invalidation generation of a lookup table. Read it before
    /// loading the map and pass it to [`Self::insert_map`].
    pub fn generation(&self, lookup: &str) -> LookupGeneration {
        let maps = self.maps.read();
        LookupGeneration {
            epoch: maps.epoch,
            counter: maps.generations.get(lookup).copied().unwrap_or(0),
        }
    }

    /// Install a freshly loaded lookup table map.
    ///
    /// Returns false without installing if the lookup table was
    /// invalidated after `generation` was read: the map may predate the
    /// write that caused the invalidation, so the caller must re-read.
    pub fn insert_map(
        &self,
        query: &str,
        lookup: &str,
        entries: HashMap<String, Arc<str>>,
        generation: LookupGeneration,
    ) -> bool {
        let mut maps = self.maps.write();

        let current = LookupGeneration {
            epoch: maps.epoch,
            counter: maps.generations.get(lookup).copied().unwrap_or(0),
        };
        if current != generation {
            return false;
        }

        maps.maps.insert(
            query.to_owned(),
            TableMap {
                lookup: lookup.to_owned(),
                entries,
                loaded_at: Instant::now(),
            },
        );

        true
    }

    /// Reconcile with the configured lookups, given as (lookup table,
    /// query) pairs. If the lookup configuration changed, all loaded
    /// maps and invalidation state are dropped, so maps reload cold
    /// under the new config, and the epoch bump rejects in-flight
    /// loads that started under the old one. Cheap no-op when the
    /// configuration is unchanged.
    pub fn update_config(&self, mut lookups: Vec<(String, String)>) {
        lookups.sort();
        lookups.dedup();

        let mut maps = self.maps.write();
        if maps.config == lookups {
            return;
        }

        maps.config = lookups;
        maps.epoch += 1;
        maps.maps.clear();
        maps.generations.clear();
    }

    /// Claim a lookup table load so only one client runs the load query
    /// at a time. If another client already holds the claim, returns
    /// the notification that fires when it's released.
    pub fn claim(&'static self, query: &str) -> ResolveClaim {
        match self.in_flight.lock().entry(query.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(Notify::new()));
                ResolveClaim::Run(ResolveGuard {
                    cache: self,
                    query: query.to_owned(),
                })
            }
            Entry::Occupied(entry) => ResolveClaim::Wait(entry.get().clone()),
        }
    }

    /// Drop the maps loaded from the given lookup table and bump its
    /// generation, invalidating in-flight loads. Called when a write to
    /// the lookup table completes.
    pub fn flush_lookup_table(&self, lookup_table: &str) {
        let mut maps = self.maps.write();
        *maps.generations.entry(lookup_table.to_owned()).or_insert(0) += 1;
        maps.maps.retain(|_, map| map.lookup != lookup_table);
    }

    /// Reject or allow writes to lookup tables, e.g. while resharding
    /// depends on a warmed map staying exact.
    pub fn pause_writes(&self, paused: bool) {
        self.writes_paused.store(paused, Ordering::SeqCst);
    }

    /// Writes to lookup tables are currently rejected.
    pub fn writes_paused(&self) -> bool {
        self.writes_paused.load(Ordering::SeqCst)
    }

    /// Backdate a loaded map so a miss requires verification. Test-only.
    #[cfg(test)]
    fn backdate(&self, query: &str, by: Duration) {
        if let Some(map) = self.maps.write().maps.get_mut(query) {
            map.loaded_at -= by;
        }
    }
}

/// Record lookup tables invalidated by a write to the given table.
/// Cheap no-op when no lookups are configured.
///
/// The caller flushes them when the write *completes* (statement, or
/// COMMIT for transactions) — flushing at parse time would let a
/// concurrent load read pre-commit data and outlive the flush.
///
/// The configured lookup table may be schema-qualified while the parser
/// reports bare table names, so only the table name component is compared.
pub fn written_lookup_tables(schema: &ShardingSchema, written: &str, out: &mut Vec<String>) {
    if !schema.tables.has_lookups() {
        return;
    }

    for table in schema.tables.tables() {
        if let Some(lookup) = table.lookup.as_deref()
            && lookup.rsplit('.').next() == Some(written)
            && !out.iter().any(|recorded| recorded == lookup)
        {
            out.push(lookup.to_owned());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::ShardedTables;
    use crate::frontend::router::sharding::ShardedTable;
    use pgdog_config::SystemCatalogsBehavior;

    // The cache is global and tests run in parallel: every test uses
    // query strings and lookup table names unique to it so tests don't
    // clobber each other's maps.
    fn load(query: &str, lookup: &str, entries: &[(&str, &str)]) {
        let cache = LookupCache::get();
        let entries = entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), Arc::from(*value)))
            .collect();
        assert!(cache.insert_map(query, lookup, entries, cache.generation(lookup)));
    }

    #[test]
    fn test_map_hit_and_missing() {
        let cache = LookupCache::get();
        let query = "SELECT k, v FROM roots_hit";

        assert_eq!(cache.lookup(query, "child"), MapLookup::NotLoaded);
        assert!(!cache.loaded(query));

        load(query, "roots_hit", &[("child", "root")]);

        assert!(cache.loaded(query));
        assert_eq!(cache.lookup(query, "child"), MapLookup::Hit("root".into()));
        // Fresh map: absence is authoritative.
        assert_eq!(cache.lookup(query, "unknown"), MapLookup::Missing);
    }

    #[test]
    fn test_map_miss_requires_verification_when_old() {
        let cache = LookupCache::get();
        let query = "SELECT k, v FROM roots_stale";

        load(query, "roots_stale", &[("child", "root")]);
        cache.backdate(query, MISS_VERIFY_COOLDOWN * 2);

        // Hits don't need verification.
        assert_eq!(cache.lookup(query, "child"), MapLookup::Hit("root".into()));
        // Misses against an old map do: the row may have been added
        // through another PgDog instance since the map was loaded.
        assert_eq!(cache.lookup(query, "unknown"), MapLookup::Stale);
    }

    #[test]
    fn test_insert_map_rejected_after_flush() {
        let cache = LookupCache::get();
        let query = "SELECT k, v FROM roots_generation";

        // A load that started before the invalidation can't install.
        let generation = cache.generation("roots_generation");
        cache.flush_lookup_table("roots_generation");
        assert!(!cache.insert_map(
            query,
            "roots_generation",
            HashMap::from([("child".to_string(), Arc::from("stale"))]),
            generation,
        ));
        assert_eq!(cache.lookup(query, "child"), MapLookup::NotLoaded);

        // Re-reading the generation lets the retried load through.
        load(query, "roots_generation", &[("child", "fresh")]);
        assert_eq!(cache.lookup(query, "child"), MapLookup::Hit("fresh".into()));
    }

    #[test]
    fn test_update_config_clears_on_change() {
        // A local instance: update_config drops all maps, which would
        // clobber parallel tests sharing the global cache.
        let cache = LookupCache::new();
        let query = "SELECT k, v FROM roots_config";
        let pair = || vec![("roots_config".to_string(), query.to_string())];
        let entries = HashMap::from([("child".to_string(), Arc::from("root"))]);

        cache.update_config(pair());
        let generation = cache.generation("roots_config");
        assert!(cache.insert_map(query, "roots_config", entries.clone(), generation));

        // Same config: maps survive.
        cache.update_config(pair());
        assert_eq!(cache.lookup(query, "child"), MapLookup::Hit("root".into()));

        // Changed config: maps drop, and a load that started under the
        // old config can't install, even though the lookup table was
        // never invalidated.
        let generation = cache.generation("roots_config");
        cache.update_config(vec![]);
        assert_eq!(cache.lookup(query, "child"), MapLookup::NotLoaded);
        assert!(!cache.insert_map(query, "roots_config", entries, generation));
    }

    #[test]
    fn test_flush_lookup_table() {
        let cache = LookupCache::get();
        let query = "SELECT k, v FROM roots_flush";
        let other = "SELECT k, v FROM roots_flush_other";

        load(query, "roots_flush", &[("child", "root")]);
        load(other, "roots_flush_other", &[("child", "root")]);

        cache.flush_lookup_table("roots_flush");

        assert_eq!(cache.lookup(query, "child"), MapLookup::NotLoaded);
        assert_eq!(cache.lookup(other, "child"), MapLookup::Hit("root".into()));
    }

    #[test]
    fn test_claim_release() {
        let cache = LookupCache::get();
        let query = "SELECT k, v FROM roots_claim";

        let guard = match cache.claim(query) {
            ResolveClaim::Run(guard) => guard,
            ResolveClaim::Wait(_) => panic!("claim should be free"),
        };

        // Concurrent claims on the same load wait while the first is held.
        assert!(matches!(cache.claim(query), ResolveClaim::Wait(_)));

        drop(guard);
        assert!(matches!(cache.claim(query), ResolveClaim::Run(_)));
    }

    #[tokio::test]
    async fn test_claim_wakes_waiters() {
        let cache = LookupCache::get();
        let query = "SELECT k, v FROM roots_claim_wake";

        let guard = match cache.claim(query) {
            ResolveClaim::Run(guard) => guard,
            ResolveClaim::Wait(_) => panic!("claim should be free"),
        };
        let notify = match cache.claim(query) {
            ResolveClaim::Wait(notify) => notify,
            ResolveClaim::Run(_) => panic!("claim should be held"),
        };

        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        drop(guard);

        // Completes only if dropping the guard woke us.
        tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .expect("guard drop should wake waiters");
    }

    #[test]
    fn test_pause_writes() {
        let cache = LookupCache::get();

        assert!(!cache.writes_paused());
        cache.pause_writes(true);
        assert!(cache.writes_paused());
        cache.pause_writes(false);
        assert!(!cache.writes_paused());
    }

    fn record_test_schema(lookup: &str, query: &str) -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    column: "org_id".into(),
                    name: Some("lookup_cache_test".into()),
                    lookup: Some(lookup.into()),
                    query: Some(query.into()),
                    ..Default::default()
                }],
                vec![],
                false,
                SystemCatalogsBehavior::default(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn test_written_lookup_tables() {
        let schema = record_test_schema(
            "roots_written",
            "SELECT org_id, root_org_id FROM roots_written",
        );

        let mut written = Vec::new();

        // Writing an unrelated table records nothing.
        written_lookup_tables(&schema, "some_other_table", &mut written);
        assert!(written.is_empty());

        // Writing the lookup table records it, once.
        written_lookup_tables(&schema, "roots_written", &mut written);
        written_lookup_tables(&schema, "roots_written", &mut written);
        assert_eq!(written, vec!["roots_written".to_string()]);
    }

    #[test]
    fn test_written_lookup_tables_schema_qualified() {
        let schema = record_test_schema(
            "app.roots_qualified",
            "SELECT org_id, root_org_id FROM app.roots_qualified",
        );

        // The parser reports bare table names; a schema-qualified
        // lookup config still matches, and the recorded name is the
        // configured one so it matches the loaded maps.
        let mut written = Vec::new();
        written_lookup_tables(&schema, "roots_qualified", &mut written);
        assert_eq!(written, vec!["app.roots_qualified".to_string()]);
    }
}
