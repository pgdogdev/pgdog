use std::{num::NonZeroUsize, sync::Arc};

use cachekit::traits::Cache as _;
use pgdog_cache::CachePolicy;

pub type Key = u64;
pub type Value = u64;

/// A cache under benchmark.
pub trait Cache {
    /// Names the cache in benchmark IDs.
    const NAME: &'static str;

    /// Creates a cache for `capacity` entries.
    fn new(capacity: usize) -> Self;

    /// Inserts `value` for `key` in the cache.
    fn insert(&mut self, key: Key, value: Value);

    /// Returns the value for `key`, recording a use.
    fn get(&mut self, key: &Key) -> Option<Value>;

    /// Evicts entries until at most `capacity` remain.
    /// Does nothing for caches that evict on their own.
    fn evict_to(&mut self, capacity: usize) {
        let _ = capacity;
    }
}

pub trait Peek: Cache {
    /// Returns the value for `key` without recording a use.
    fn peek(&self, key: &Key) -> Option<Value>;
}

pub trait Remove: Cache {
    /// Removes `key`, returning whether it was present.
    fn remove(&mut self, key: &Key) -> bool;
}

pub trait Pop: Cache {
    /// Removes and returns the next entry in eviction order.
    fn pop(&mut self) -> Option<(Key, Value)>;
}

fn non_zero(capacity: usize) -> NonZeroUsize {
    NonZeroUsize::new(capacity).expect("capacity must be non-zero")
}

// -------------------------------------------------------
// pgdog-cache
// -------------------------------------------------------

pub struct PgdogLru(pgdog_cache::Cache<Key, Value>);

impl Cache for PgdogLru {
    const NAME: &'static str = "pgdog-lru";

    fn new(capacity: usize) -> Self {
        Self(pgdog_cache::Cache::with_capacity(capacity))
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.insert(key, value);
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).copied()
    }

    fn evict_to(&mut self, capacity: usize) {
        while self.0.len() > capacity {
            self.0.pop();
        }
    }
}

impl Peek for PgdogLru {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).copied()
    }
}

impl Remove for PgdogLru {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.remove(key).is_some()
    }
}

impl Pop for PgdogLru {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop()
    }
}

pub struct PgdogLfu(pgdog_cache::Cache<Key, Value>);

impl Cache for PgdogLfu {
    const NAME: &'static str = "pgdog-lfu";

    fn new(capacity: usize) -> Self {
        let mut cache = pgdog_cache::Cache::with_capacity(capacity);
        cache.configure(CachePolicy::LeastFrequentlyUsed);

        Self(cache)
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.insert(key, value);
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).copied()
    }

    fn evict_to(&mut self, capacity: usize) {
        while self.0.len() > capacity {
            self.0.pop();
        }
    }
}

impl Peek for PgdogLfu {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).copied()
    }
}

impl Remove for PgdogLfu {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.remove(key).is_some()
    }
}

impl Pop for PgdogLfu {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop()
    }
}

// -------------------------------------------------------
// LRU
// -------------------------------------------------------

pub struct Lru(lru::LruCache<Key, Value>);

impl Cache for Lru {
    const NAME: &'static str = "lru";

    fn new(_capacity: usize) -> Self {
        // Can't preallocate while unbounded.
        Self(lru::LruCache::unbounded())
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.put(key, value);
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).copied()
    }

    fn evict_to(&mut self, capacity: usize) {
        while self.0.len() > capacity {
            self.0.pop_lru();
        }
    }
}

impl Peek for Lru {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).copied()
    }
}

impl Remove for Lru {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.pop(key).is_some()
    }
}

impl Pop for Lru {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop_lru()
    }
}

pub struct Schnellru(schnellru::LruMap<Key, Value, schnellru::Unlimited>);

impl Cache for Schnellru {
    const NAME: &'static str = "schnellru";

    fn new(capacity: usize) -> Self {
        let mut map = schnellru::LruMap::new(schnellru::Unlimited);
        map.reserve_or_panic(capacity);

        Self(map)
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.insert(key, value);
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).copied()
    }

    fn evict_to(&mut self, capacity: usize) {
        while self.0.len() > capacity {
            self.0.pop_oldest();
        }
    }
}

impl Peek for Schnellru {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).copied()
    }
}

impl Remove for Schnellru {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.remove(key).is_some()
    }
}

impl Pop for Schnellru {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop_oldest()
    }
}

pub struct CachekitLru(cachekit::policy::fast_lru::FastLru<Key, Value>);

impl Cache for CachekitLru {
    const NAME: &'static str = "cachekit-lru";

    fn new(capacity: usize) -> Self {
        Self(cachekit::policy::fast_lru::FastLru::new(capacity))
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.insert(key, value);
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).copied()
    }
}

impl Peek for CachekitLru {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).copied()
    }
}

impl Remove for CachekitLru {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.remove(key).is_some()
    }
}

impl Pop for CachekitLru {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop_lru()
    }
}

pub struct EvictorLru(evictor::Lru<Key, Value>);

impl Cache for EvictorLru {
    const NAME: &'static str = "evictor-lru";

    fn new(capacity: usize) -> Self {
        Self(evictor::Lru::new(non_zero(capacity)))
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.insert(key, value);
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).copied()
    }
}

impl Peek for EvictorLru {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).copied()
    }
}

impl Remove for EvictorLru {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.remove(key).is_some()
    }
}

impl Pop for EvictorLru {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop()
    }
}

// -------------------------------------------------------
// LFU
// -------------------------------------------------------

pub struct CachekitLfu(cachekit::policy::lfu::LfuCache<Key, Value>);

impl Cache for CachekitLfu {
    const NAME: &'static str = "cachekit-lfu";

    fn new(capacity: usize) -> Self {
        Self(cachekit::policy::lfu::LfuCache::new(capacity))
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.insert(key, Arc::new(value));
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).map(|value| **value)
    }
}

impl Peek for CachekitLfu {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).map(|value| **value)
    }
}

impl Remove for CachekitLfu {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.remove(key).is_some()
    }
}

impl Pop for CachekitLfu {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop_lfu().map(|(key, value)| (key, *value))
    }
}

pub struct EvictorLfu(evictor::Lfu<Key, Value>);

impl Cache for EvictorLfu {
    const NAME: &'static str = "evictor-lfu";

    fn new(capacity: usize) -> Self {
        Self(evictor::Lfu::new(non_zero(capacity)))
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.0.insert(key, value);
    }

    fn get(&mut self, key: &Key) -> Option<Value> {
        self.0.get(key).copied()
    }
}

impl Peek for EvictorLfu {
    fn peek(&self, key: &Key) -> Option<Value> {
        self.0.peek(key).copied()
    }
}

impl Remove for EvictorLfu {
    fn remove(&mut self, key: &Key) -> bool {
        self.0.remove(key).is_some()
    }
}

impl Pop for EvictorLfu {
    fn pop(&mut self) -> Option<(Key, Value)> {
        self.0.pop()
    }
}
