//! Sharding key lookup cache.
//!
//! Tables configured with a `lookup_query` translate sharding key values
//! through it, e.g. to route child tenants to their parent's shard.
//! Results are cached here, keyed by the value. Only translations are
//! cached: a value without a row fails the statement and is looked up
//! again on the next one, so rows added later are picked up without
//! any invalidation.
//!
//! The cache is scoped to a cluster, like
//! [`crate::backend::schema::SchemaCache`], so it's recreated,
//! i.e. emptied, on config reload.
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use moka::sync::Cache;
use tracing::warn;

use crate::backend::{Cluster, ShardingSchema};
use crate::frontend::router::parser::Shard;
use crate::frontend::router::sharding::{ContextBuilder, ShardedTable};
use crate::net::bind::Parameter;
use crate::net::messages::ErrorResponse;
use crate::util::safe_timeout;
use pgdog_config::LookupResult;

/// How much memory the cache is allowed to use, approximately,
/// unless configured with `sharding_lookup_cache_size`.
const LOOKUP_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Approximate memory used by an entry on top of its strings:
/// hash table slot, eviction bookkeeping and allocation headers.
const LOOKUP_ENTRY_OVERHEAD: usize = 80;

/// Identifies the sharded table a translation belongs to. Together
/// with the cluster-scoped cache, this fully qualifies the entry:
/// two tables with identical lookup queries can't collide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LookupTable {
    /// Table schema, if configured.
    pub(crate) schema: Option<String>,
    /// Table name, if configured.
    pub(crate) name: Option<String>,
    /// Sharded column name.
    pub(crate) column: String,
}

impl From<&ShardedTable> for LookupTable {
    fn from(table: &ShardedTable) -> Self {
        Self {
            schema: table.schema.clone(),
            name: table.name.clone(),
            column: table.column.clone(),
        }
    }
}

/// Owned key of a cached translation. Hashes and compares through
/// its [`KeyView`], so lookups can run with fully borrowed keys.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheKey {
    table: LookupTable,
    value: String,
}

impl Hash for CacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self as &dyn KeyView).hash(state)
    }
}

impl<'a> Borrow<dyn KeyView + 'a> for CacheKey {
    fn borrow(&self) -> &(dyn KeyView + 'a) {
        self
    }
}

/// Borrowed view of a cache key. The hot paths (routing, COPY rows,
/// replicated changes) get with borrowed table identity and value:
/// building an owned key per get would allocate up to four strings on
/// every cache hit, which is what the cache exists to avoid.
trait KeyView {
    fn schema(&self) -> Option<&str>;
    fn name(&self) -> Option<&str>;
    fn column(&self) -> &str;
    fn value(&self) -> &str;
}

impl KeyView for CacheKey {
    fn schema(&self) -> Option<&str> {
        self.table.schema.as_deref()
    }
    fn name(&self) -> Option<&str> {
        self.table.name.as_deref()
    }
    fn column(&self) -> &str {
        &self.table.column
    }
    fn value(&self) -> &str {
        &self.value
    }
}

impl KeyView for (&LookupTable, &str) {
    fn schema(&self) -> Option<&str> {
        self.0.schema.as_deref()
    }
    fn name(&self) -> Option<&str> {
        self.0.name.as_deref()
    }
    fn column(&self) -> &str {
        &self.0.column
    }
    fn value(&self) -> &str {
        self.1
    }
}

impl KeyView for (&ShardedTable, &str) {
    fn schema(&self) -> Option<&str> {
        self.0.schema.as_deref()
    }
    fn name(&self) -> Option<&str> {
        self.0.name.as_deref()
    }
    fn column(&self) -> &str {
        &self.0.column
    }
    fn value(&self) -> &str {
        self.1
    }
}

impl Hash for dyn KeyView + '_ {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.schema().hash(state);
        self.name().hash(state);
        self.column().hash(state);
        self.value().hash(state);
    }
}

impl PartialEq for dyn KeyView + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.schema() == other.schema()
            && self.name() == other.name()
            && self.column() == other.column()
            && self.value() == other.value()
    }
}

impl Eq for dyn KeyView + '_ {}

/// A lookup recorded on a cache miss, resolved by the query engine
/// before the statement routes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PendingLookup {
    /// The sharded table the value routes through.
    pub(crate) table: LookupTable,
    /// Lookup query, with a `$1` placeholder for the value.
    pub(crate) query: String,
    /// Sharding key value, normalized to text.
    pub(crate) value: String,
}

/// How a bare sharding key value (a `pgdog_sharding_key` comment or
/// `SET pgdog.sharding_key`) routes: either straight to a shard, or
/// through a lookup that has to run first.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ShardOrLookup {
    /// The value hashed to this shard.
    Shard(Shard),
    /// The value must be translated through a lookup before hashing.
    /// The query engine resolves it and routes the statement again.
    Lookup(PendingLookup),
}

/// Sharding key translations resolved for a single statement, handed
/// to the second routing pass through the router context. Routing
/// reads these before the lookup cache, so the second pass can't miss:
/// parsing is deterministic, and every key it asks for was collected,
/// resolved and recorded here by the first pass. The cache remains a
/// cross-statement optimization; correctness doesn't depend on it
/// retaining anything.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResolvedLookups {
    map: HashMap<CacheKey, Arc<str>>,
}

impl ResolvedLookups {
    /// Get the translation resolved for a sharding key value.
    pub(crate) fn get(&self, table: &LookupTable, value: &str) -> Option<Arc<str>> {
        self.get_view(&(table, value))
    }

    /// Like [`Self::get`], keyed straight off the sharded table.
    pub(crate) fn get_for_table(&self, table: &ShardedTable, value: &str) -> Option<Arc<str>> {
        self.get_view(&(table, value))
    }

    fn get_view(&self, key: &dyn KeyView) -> Option<Arc<str>> {
        // The first routing pass always runs with an empty map: only
        // its misses fill it for the second pass.
        if self.map.is_empty() {
            return None;
        }
        self.map.get(key).cloned()
    }

    pub(crate) fn insert(&mut self, table: LookupTable, value: String, translated: Arc<str>) {
        self.map.insert(CacheKey { table, value }, translated);
    }
}

/// Interpret a lookup query result as a shard number: `lookup_result
/// = "shard"` tables never hash. Strict: the value must be a 0-based
/// index within the cluster's shard count, anything else fails the
/// statement, never routing it by a wrong or wrapped shard.
pub(crate) fn parse_shard_index(translated: &str, shards: usize) -> Result<Shard, super::Error> {
    let shard = translated
        .parse::<usize>()
        .map_err(|_| super::Error::LookupShardInvalid(translated.to_owned()))?;
    if shard >= shards {
        return Err(super::Error::LookupShardOutOfRange { shard, shards });
    }
    Ok(Shard::Direct(shard))
}

/// Route a bare sharding key value through the common mapping,
/// translating it through the common lookup first when one is
/// configured, same as values extracted from statements. Translations
/// resolved for this statement are consulted before the cache.
pub(crate) fn shard_for_bare_key(
    value: &str,
    schema: &ShardingSchema,
    resolved: Option<&ResolvedLookups>,
) -> Result<ShardOrLookup, super::Error> {
    if let Some(mapping) = schema.tables.common_mapping()
        && let Some(lookup) = &mapping.lookup
    {
        let translated = resolved
            .and_then(|resolved| resolved.get(&lookup.table, value))
            .or_else(|| schema.tables.lookup_cache().get(&lookup.table, value));
        match translated {
            Some(translated) => {
                // Shard-mode lookups never hash: the translation is
                // the shard number.
                if lookup.result == LookupResult::Shard {
                    return Ok(ShardOrLookup::Shard(parse_shard_index(
                        translated.as_ref(),
                        schema.shards,
                    )?));
                }
                let ctx = ContextBuilder::infer_from_from_and_config(translated.as_ref(), schema)?
                    .shards(schema.shards)
                    .build()?;
                return Ok(ShardOrLookup::Shard(ctx.apply()?));
            }
            None => {
                return Ok(ShardOrLookup::Lookup(PendingLookup {
                    table: lookup.table.clone(),
                    query: lookup.query.clone(),
                    value: value.to_owned(),
                }));
            }
        }
    }

    let ctx = ContextBuilder::infer_from_from_and_config(value, schema)?
        .shards(schema.shards)
        .build()?;
    Ok(ShardOrLookup::Shard(ctx.apply()?))
}

/// Second chance for a pending bare-key lookup at routing time: the
/// comment directive is parsed before routing and cached with the
/// statement, so its pending lookup is stale by the second routing
/// pass. Returns the shard if the translation has been resolved for
/// this statement or is cached, `None` if the lookup still has to run.
pub(crate) fn shard_for_pending(
    pending: &PendingLookup,
    schema: &ShardingSchema,
    resolved: &ResolvedLookups,
) -> Result<Option<Shard>, super::Error> {
    let translated = resolved.get(&pending.table, &pending.value).or_else(|| {
        schema
            .tables
            .lookup_cache()
            .get(&pending.table, &pending.value)
    });
    match translated {
        Some(translated) => {
            // Pendings here only originate from the common lookup,
            // which carries the interpretation. Shard-mode lookups
            // never hash: the translation is the shard number.
            let result = schema
                .tables
                .common_mapping()
                .as_ref()
                .and_then(|mapping| mapping.lookup.as_ref())
                .map(|lookup| lookup.result)
                .unwrap_or_default();
            if result == LookupResult::Shard {
                return Ok(Some(parse_shard_index(translated.as_ref(), schema.shards)?));
            }
            let ctx = ContextBuilder::infer_from_from_and_config(translated.as_ref(), schema)?
                .shards(schema.shards)
                .build()?;
            Ok(Some(ctx.apply()?))
        }
        None => Ok(None),
    }
}

/// Sharding key lookup metrics: cache effectiveness and the cost of
/// misses. Shared between the cluster's lookup cache, which records
/// them, and the cluster's metrics, which report them.
#[derive(Debug, Default)]
pub(crate) struct LookupStats {
    /// Cache hits.
    pub(crate) hits: AtomicU64,
    /// Cache misses.
    pub(crate) misses: AtomicU64,
    /// Entries evicted to stay within the memory bound.
    pub(crate) evictions: AtomicU64,
    /// Lookup queries run: misses resolved by querying a shard.
    pub(crate) lookups: AtomicU64,
    /// Total time spent running lookup queries, in microseconds.
    /// Divided by `lookups`, the average lookup latency.
    pub(crate) lookup_time_us: AtomicU64,
}

impl LookupStats {
    fn record_lookup(&self, elapsed: Duration) {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        self.lookup_time_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    /// Zero all counters, used by the RESET STATS command.
    pub(crate) fn reset(&self) {
        for counter in [
            &self.hits,
            &self.misses,
            &self.evictions,
            &self.lookups,
            &self.lookup_time_us,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }

    /// Add another snapshot of counters into this one, used when a config
    /// reload carries lookup statistics over to the new cluster's cache.
    pub(crate) fn accumulate(&self, other: &LookupStats) {
        for (to, from) in [
            (&self.hits, &other.hits),
            (&self.misses, &other.misses),
            (&self.evictions, &other.evictions),
            (&self.lookups, &other.lookups),
            (&self.lookup_time_us, &other.lookup_time_us),
        ] {
            to.fetch_add(from.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }
}

/// Sharding key lookup cache. Bounded by approximate memory use;
/// the least recently used translations are evicted first.
#[derive(Debug, Clone)]
pub(crate) struct LookupCache {
    // (Sharded table, sharding key value) => translated value.
    cache: Cache<CacheKey, Arc<str>>,
    stats: Arc<LookupStats>,
}

impl Default for LookupCache {
    fn default() -> Self {
        Self::new(LOOKUP_CACHE_MAX_BYTES)
    }
}

impl LookupCache {
    /// Create a cache bounded to approximately `max_bytes` of memory.
    /// The least recently used translations are evicted when full.
    pub(crate) fn new(max_bytes: u64) -> Self {
        let stats = Arc::new(LookupStats::default());
        let evictions = stats.clone();
        Self {
            cache: Cache::builder()
                .weigher(|key: &CacheKey, translated: &Arc<str>| {
                    let table = key.schema().map(str::len).unwrap_or(0)
                        + key.name().map(str::len).unwrap_or(0)
                        + key.column().len();
                    (LOOKUP_ENTRY_OVERHEAD + table + key.value().len() + translated.len())
                        .try_into()
                        .unwrap_or(u32::MAX)
                })
                .max_capacity(max_bytes)
                .eviction_listener(move |_key, _value, cause| {
                    if cause.was_evicted() {
                        evictions.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                })
                .build(),
            stats,
        }
    }

    /// Get the cached translation for a sharding key value.
    /// `None` means the value hasn't been translated yet.
    pub(crate) fn get(&self, table: &LookupTable, value: &str) -> Option<Arc<str>> {
        self.get_view(&(table, value))
    }

    /// Like [`Self::get`], keyed straight off the sharded table: the
    /// hit path allocates nothing.
    pub(crate) fn get_for_table(&self, table: &ShardedTable, value: &str) -> Option<Arc<str>> {
        self.get_view(&(table, value))
    }

    fn get_view(&self, key: &dyn KeyView) -> Option<Arc<str>> {
        let translated = self.cache.get(key);
        match translated {
            Some(_) => self.stats.hits.fetch_add(1, Ordering::Relaxed),
            None => self.stats.misses.fetch_add(1, Ordering::Relaxed),
        };
        translated
    }

    /// Cache the result of a lookup query.
    pub(crate) fn insert(&self, table: LookupTable, value: String, translated: Arc<str>) {
        self.cache.insert(CacheKey { table, value }, translated);
    }

    /// Lookup metrics recorded by this cache.
    pub(crate) fn stats(&self) -> &Arc<LookupStats> {
        &self.stats
    }

    /// Approximate memory used by cached translations, in bytes,
    /// and the number of entries. Runs the cache's pending
    /// maintenance first so the numbers are current.
    pub(crate) fn size(&self) -> (u64, u64) {
        self.cache.run_pending_tasks();
        (self.cache.weighted_size(), self.cache.entry_count())
    }
}

/// Resolve sharding key lookups by running the configured lookup query
/// on a single shard, picked round-robin: the lookup table is required
/// to be omnisharded, so any shard is authoritative.
///
/// A row translates the value, cached keyed by the value in the
/// cluster-scoped lookup cache, which is recreated on config reload.
/// No row fails the statement: routing by the untranslated value
/// could put it on the wrong shard, e.g. when the row just hasn't
/// been inserted yet. Absence isn't cached, so the value is looked
/// up again on the next statement and rows added later are picked
/// up without any invalidation.
///
/// Query failures are returned as an error for the client too.
pub(crate) async fn resolve(
    cluster: &Cluster,
    pending: Vec<PendingLookup>,
) -> Result<ResolvedLookups, ErrorResponse> {
    let schema = cluster.sharding_schema();
    resolve_with(cluster, schema.tables.lookup_cache(), pending).await
}

/// Like [`resolve`], with the cluster running the queries decoupled
/// from the cache the results fill. Resharding data sync runs lookups
/// on the source cluster, which has the complete mapping while the
/// destination is still syncing, and fills the destination's cache,
/// which the row sharder reads.
pub(crate) async fn resolve_with(
    cluster: &Cluster,
    cache: &LookupCache,
    pending: Vec<PendingLookup>,
) -> Result<ResolvedLookups, ErrorResponse> {
    // A statement can extract the same value more than once,
    // e.g. a multi-row INSERT with repeated values.
    let pending = pending.into_iter().collect::<HashSet<_>>();
    let mut resolved = ResolvedLookups::default();

    for lookup in pending {
        let translated = resolve_one(cluster, cache, &lookup).await?;
        resolved.insert(lookup.table, lookup.value, translated);
    }

    Ok(resolved)
}

/// Resolve a single sharding key lookup and return the translated
/// value, from the cache or by running the lookup query.
pub(crate) async fn resolve_one(
    cluster: &Cluster,
    cache: &LookupCache,
    lookup: &PendingLookup,
) -> Result<Arc<str>, ErrorResponse> {
    // Another statement could have resolved it in the meantime.
    if let Some(translated) = cache.get(&lookup.table, &lookup.value) {
        return Ok(translated);
    }

    run_lookup(cluster, cache, &lookup.table, &lookup.query, &lookup.value).await
}

/// Like [`resolve_one`], keyed straight off the sharded table: the
/// cache-hit path allocates nothing. Per-row callers (COPY) use this.
pub(crate) async fn resolve_for_table(
    cluster: &Cluster,
    cache: &LookupCache,
    table: &ShardedTable,
    value: &str,
) -> Result<Arc<str>, ErrorResponse> {
    let query = table
        .lookup_query
        .as_deref()
        .ok_or_else(|| ErrorResponse::sharding_key_lookup("no lookup query configured"))?;

    if let Some(translated) = cache.get_for_table(table, value) {
        return Ok(translated);
    }

    run_lookup(cluster, cache, &LookupTable::from(table), query, value).await
}

/// The cache-miss path: run the lookup query and cache the row.
async fn run_lookup(
    cluster: &Cluster,
    cache: &LookupCache,
    table: &LookupTable,
    query: &str,
    value: &str,
) -> Result<Arc<str>, ErrorResponse> {
    let params = [Parameter::new(value.as_bytes())];
    let start = Instant::now();
    let rows = safe_timeout(
        cluster.sharding_lookup_timeout(),
        cluster.fetch_all_round_robin::<String>(query, &params),
    )
    .await;
    cache.stats().record_lookup(start.elapsed());
    let rows = rows
        .map_err(|_| {
            warn!("sharding key lookup \"{}\" timed out", query);
            ErrorResponse::sharding_key_lookup("lookup query timed out")
        })?
        .map_err(|err| {
            warn!("sharding key lookup \"{}\" failed: {}", query, err);
            ErrorResponse::sharding_key_lookup(&format!("lookup query failed: {}", err))
        })?;

    match rows.into_iter().next() {
        Some(row) => {
            let translated: Arc<str> = Arc::from(row.as_str());
            cache.insert(table.clone(), value.to_owned(), translated.clone());
            Ok(translated)
        }
        None => Err(ErrorResponse::sharding_key_lookup(&format!(
            "no row for value \"{}\"",
            value
        ))),
    }
}

#[cfg(test)]
mod test_support {
    use super::*;

    pub(super) fn table() -> LookupTable {
        LookupTable {
            schema: None,
            name: Some("tenants".into()),
            column: "tenant_id".into(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::test_support::table;
    use super::*;

    #[test]
    fn test_borrowed_keys_hash_like_owned() {
        // The allocation-free gets rely on the borrowed key views
        // hashing and comparing exactly like the owned CacheKey.
        let cache = LookupCache::default();
        let resolved = {
            let mut resolved = ResolvedLookups::default();
            resolved.insert(table(), "k1".into(), Arc::from("v1"));
            resolved
        };
        cache.insert(table(), "k1".into(), Arc::from("v1"));

        let sharded = ShardedTable {
            schema: table().schema,
            name: table().name,
            column: table().column,
            ..Default::default()
        };
        assert_eq!(cache.get(&table(), "k1").as_deref(), Some("v1"));
        assert_eq!(cache.get_for_table(&sharded, "k1").as_deref(), Some("v1"));
        assert_eq!(resolved.get(&table(), "k1").as_deref(), Some("v1"));
        assert_eq!(
            resolved.get_for_table(&sharded, "k1").as_deref(),
            Some("v1")
        );

        // A different table identity misses even with the same value.
        let other = ShardedTable {
            column: "other".into(),
            ..Default::default()
        };
        assert_eq!(cache.get_for_table(&other, "k1"), None);
        assert_eq!(resolved.get_for_table(&other, "k1"), None);

        // Fields with None schema/name hash consistently too.
        let bare = LookupTable {
            schema: None,
            name: None,
            column: "c".into(),
        };
        cache.insert(bare.clone(), "k2".into(), Arc::from("v2"));
        let bare_sharded = ShardedTable {
            column: "c".into(),
            ..Default::default()
        };
        assert_eq!(cache.get(&bare, "k2").as_deref(), Some("v2"));
        assert_eq!(
            cache.get_for_table(&bare_sharded, "k2").as_deref(),
            Some("v2")
        );
    }

    #[test]
    fn test_insert_get() {
        let cache = LookupCache::default();

        assert_eq!(cache.get(&table(), "child"), None);

        cache.insert(table(), "child".into(), "root".into());
        assert_eq!(cache.get(&table(), "child"), Some("root".into()));

        // Same query, different table: no collision.
        let other = LookupTable {
            name: Some("customers".into()),
            ..table()
        };
        assert_eq!(cache.get(&other, "child"), None);
    }

    #[test]
    fn test_clones_share_entries() {
        let cache = LookupCache::default();
        let clone = cache.clone();

        cache.insert(table(), "child".into(), "root".into());
        assert_eq!(clone.get(&table(), "child"), Some("root".into()));
    }
}

#[cfg(test)]
mod stats_test {
    use super::test_support::table;
    use super::*;

    #[test]
    fn test_hits_and_misses_counted() {
        let cache = LookupCache::default();

        assert!(cache.get(&table(), "child").is_none());
        cache.insert(table(), "child".into(), "root".into());
        assert!(cache.get(&table(), "child").is_some());
        assert!(cache.get(&table(), "child").is_some());

        let stats = cache.stats();
        assert_eq!(stats.hits.load(Ordering::Relaxed), 2);
        assert_eq!(stats.misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_evictions_counted() {
        // Room for roughly one entry: the overhead alone is 80 bytes.
        let cache = LookupCache::new(150);

        for value in ["a", "b", "c", "d"] {
            cache.insert(table(), value.into(), "root".into());
            cache.cache.run_pending_tasks();
        }

        assert!(
            cache.stats().evictions.load(Ordering::Relaxed) > 0,
            "inserting past the memory bound should evict"
        );
        assert_eq!(cache.size().1, 1);
    }

    #[test]
    fn test_cluster_metrics_share_lookup_stats() {
        let cluster = Cluster::new_test(&crate::config::config());
        let stats = cluster.stats();
        let cache_stats = cluster
            .sharding_schema()
            .tables
            .lookup_cache()
            .stats()
            .clone();
        assert!(Arc::ptr_eq(&stats.lock().lookup, &cache_stats));
    }
}

#[cfg(test)]
mod shard_index_test {
    use super::*;

    #[test]
    fn test_parse_shard_index() {
        assert_eq!(parse_shard_index("2", 3).unwrap(), Shard::Direct(2));
        assert_eq!(parse_shard_index("0", 1).unwrap(), Shard::Direct(0));

        assert!(matches!(
            parse_shard_index("3", 3),
            Err(super::super::Error::LookupShardOutOfRange {
                shard: 3,
                shards: 3
            })
        ));
        assert!(matches!(
            parse_shard_index("abc", 3),
            Err(super::super::Error::LookupShardInvalid(_))
        ));
        assert!(matches!(
            parse_shard_index("-1", 3),
            Err(super::super::Error::LookupShardInvalid(_))
        ));
        assert!(matches!(
            parse_shard_index("", 3),
            Err(super::super::Error::LookupShardInvalid(_))
        ));
    }
}
