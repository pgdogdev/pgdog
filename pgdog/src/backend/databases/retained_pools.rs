use std::num::NonZeroUsize;

use lru::LruCache;

use super::User;
use crate::backend::pool::{Address, Pool};

// Bound retained state for users that never authenticate again after a reload.
const MAX_RETAINED_POOLS: usize = 4096;

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    user: User,
    shard: usize,
    address: Address,
}

impl Key {
    fn new(user: &User, shard: usize, pool: &Pool) -> Self {
        let mut address = pool.addr().clone();
        // Match the same address compatibility rules as ordinary pool reloads.
        address.passwords.clear();
        address.database_number = 0;
        Self {
            user: user.clone(),
            shard,
            address,
        }
    }
}

/// Offline pools retain counters until their user authenticates again.
/// Keeping the pool also accounts for transactions that finish after reload.
pub(super) struct RetainedPools {
    pools: LruCache<Key, Pool>,
}

impl Default for RetainedPools {
    fn default() -> Self {
        Self {
            pools: LruCache::new(
                NonZeroUsize::new(MAX_RETAINED_POOLS).expect("nonzero retention limit"),
            ),
        }
    }
}

impl RetainedPools {
    pub(super) fn insert(&mut self, user: &User, shard: usize, pool: &Pool) {
        self.pools.put(Key::new(user, shard, pool), pool.clone());
    }

    pub(super) fn take(&mut self, user: &User, shard: usize, pool: &Pool) -> Option<Pool> {
        self.pools.pop(&Key::new(user, shard, pool))
    }

    pub(super) fn clear(&mut self) {
        self.pools.clear();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::backend::pool::PoolConfig;

    #[test]
    fn retention_is_bounded_and_consumed_once() {
        let mut retained = RetainedPools {
            pools: LruCache::new(NonZeroUsize::new(2).expect("capacity")),
        };
        let user = User {
            user: "user".into(),
            database: "database".into(),
        };
        let pool = Pool::new_test();
        for shard in 0..3 {
            retained.insert(&user, shard, &pool);
        }
        assert_eq!(retained.pools.len(), 2);
        assert!(retained.take(&user, 0, &pool).is_none());
        assert_eq!(
            retained.take(&user, 1, &pool).expect("retained").id(),
            pool.id()
        );
        assert!(retained.take(&user, 1, &pool).is_none());
        retained.clear();
        assert!(retained.take(&user, 2, &pool).is_none());
    }

    #[test]
    fn retention_matches_identity_and_compatible_addresses() {
        let mut retained = RetainedPools::default();
        let user = User {
            user: "user".into(),
            database: "database".into(),
        };
        let original = Pool::new_test();
        retained.insert(&user, 1, &original);

        let mut changed = original.addr().clone();
        changed.passwords = vec!["rotated password".into()];
        changed.database_number += 1;
        let replacement = Pool::new(&PoolConfig {
            address: changed.clone(),
            ..Default::default()
        });
        assert!(
            retained
                .take(
                    &User {
                        user: "other".into(),
                        ..user.clone()
                    },
                    1,
                    &replacement
                )
                .is_none()
        );
        assert!(
            retained
                .take(
                    &User {
                        database: "other".into(),
                        ..user.clone()
                    },
                    1,
                    &replacement
                )
                .is_none()
        );
        assert!(retained.take(&user, 0, &replacement).is_none());
        changed.port += 1;
        let other_server = Pool::new(&PoolConfig {
            address: changed,
            ..Default::default()
        });
        assert!(retained.take(&user, 1, &other_server).is_none());
        assert_eq!(
            retained
                .take(&user, 1, &replacement)
                .expect("compatible pool")
                .id(),
            original.id()
        );
    }
}
