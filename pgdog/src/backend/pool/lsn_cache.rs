//! LSN stats shared between pools connected to the same server.
//!
//! `pg_is_in_recovery()` and the WAL position are the same for every
//! database on a Postgres server, so pools that share a host and port
//! reuse each other's LSN checks instead of each querying the server.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio::time::Instant;

use super::{Address, LsnStats};

static LSN_CACHE: Lazy<LsnCache> = Lazy::new(LsnCache::default);

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    host: String,
    port: u16,
}

impl From<&Address> for CacheKey {
    fn from(addr: &Address) -> Self {
        Self {
            host: addr.host.clone(),
            port: addr.port,
        }
    }
}

#[derive(Default)]
struct Entry {
    /// Latest stats and when they were stored.
    stats: Option<(Instant, LsnStats)>,
    /// Held by the pool checking the server.
    lock: Arc<AsyncMutex<()>>,
}

/// Latest LSN stats for each server, keyed by host and port.
#[derive(Default)]
pub(crate) struct LsnCache {
    entries: Mutex<HashMap<CacheKey, Entry>>,
}

impl LsnCache {
    /// Returns the global instance.
    pub(crate) fn global() -> &'static LsnCache {
        &LSN_CACHE
    }

    /// Wait until no other pool is checking the server.
    pub(crate) async fn lock(&self, addr: &Address) -> OwnedMutexGuard<()> {
        let lock = self
            .entries
            .lock()
            .entry(addr.into())
            .or_default()
            .lock
            .clone();

        lock.lock_owned().await
    }

    /// Stats stored less than `max_age` ago.
    pub(crate) fn recent(&self, addr: &Address, max_age: Duration) -> Option<LsnStats> {
        self.entries
            .lock()
            .get(&addr.into())
            .and_then(|entry| entry.stats)
            .filter(|(stored, _)| stored.elapsed() < max_age)
            .map(|(_, stats)| stats)
    }

    /// Store stats fetched from the server.
    pub(crate) fn set(&self, addr: &Address, stats: LsnStats) {
        self.entries.lock().entry(addr.into()).or_default().stats = Some((Instant::now(), stats));
    }
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use pgdog_postgres_types::TimestampTz;
    use pgdog_stats::{Lsn, LsnStats as StatsLsnStats};
    use tokio::time::timeout;

    use super::*;

    fn address(port: u16, database: &str, user: &str) -> Address {
        Address {
            port,
            database_name: database.into(),
            user: user.into(),
            ..Address::new_test()
        }
    }

    fn stats(lsn: i64) -> LsnStats {
        StatsLsnStats {
            replica: false,
            lsn: Lsn::from_i64(lsn),
            offset_bytes: lsn,
            timestamp: TimestampTz::default(),
            fetched: SystemTime::now(),
            aurora: false,
        }
        .into()
    }

    #[test]
    fn test_stats_shared_by_databases_on_same_server() {
        let cache = LsnCache::default();
        cache.set(&address(5432, "db_1", "user_1"), stats(100));

        for addr in [
            address(5432, "db_1", "user_1"),
            address(5432, "db_2", "user_1"),
            address(5432, "db_2", "user_2"),
        ] {
            let recent = cache.recent(&addr, Duration::from_secs(5));
            assert_eq!(recent.map(|stats| stats.lsn.lsn), Some(100), "{}", addr);
        }

        assert!(
            cache
                .recent(&address(5433, "db_1", "user_1"), Duration::from_secs(5))
                .is_none(),
            "different server"
        );
        assert!(
            cache
                .recent(
                    &Address {
                        host: "127.0.0.2".into(),
                        ..address(5432, "db_1", "user_1")
                    },
                    Duration::from_secs(5)
                )
                .is_none(),
            "different server"
        );
    }

    #[test]
    fn test_stale_stats_not_reused() {
        let cache = LsnCache::default();
        let addr = address(5432, "db_1", "user_1");

        assert!(cache.recent(&addr, Duration::from_secs(5)).is_none());

        cache.set(&addr, stats(100));
        assert!(cache.recent(&addr, Duration::ZERO).is_none());
        assert!(cache.recent(&addr, Duration::from_secs(5)).is_some());

        // Newer stats replace older ones.
        cache.set(&addr, stats(200));
        let recent = cache.recent(&addr, Duration::from_secs(5));
        assert_eq!(recent.map(|stats| stats.lsn.lsn), Some(200));
    }

    #[tokio::test]
    async fn test_lock_per_server() {
        let cache = LsnCache::default();
        let lock = cache.lock(&address(5432, "db_1", "user_1")).await;

        // Another database on the same server waits for the lock.
        assert!(
            timeout(
                Duration::from_millis(50),
                cache.lock(&address(5432, "db_2", "user_2"))
            )
            .await
            .is_err()
        );

        // Another server doesn't.
        assert!(
            timeout(
                Duration::from_millis(50),
                cache.lock(&address(5433, "db_1", "user_1"))
            )
            .await
            .is_ok()
        );

        drop(lock);
        assert!(
            timeout(
                Duration::from_millis(50),
                cache.lock(&address(5432, "db_2", "user_2"))
            )
            .await
            .is_ok()
        );
    }
}
