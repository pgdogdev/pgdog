use std::sync::Arc;

use scc::HashMap;

#[derive(Debug, Clone, Default)]
pub struct QueryStats {
    pub hit_count: u64,
    pub miss_count: u64,
    pub total_result_size: u64,
}

impl QueryStats {
    pub fn avg_result_size(&self) -> u64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0
        } else {
            self.total_result_size / total
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryStatsTracker {
    stats: Arc<HashMap<u64, QueryStats>>,
}

impl QueryStatsTracker {
    pub async fn record_hit(&self, cache_key_hash: u64, result_size: usize) {
        let mut entry = self.stats.entry_async(cache_key_hash).await.or_default();
        entry.hit_count += 1;
        entry.total_result_size += result_size as u64;
    }

    pub async fn record_miss(&self, cache_key_hash: u64) {
        let mut entry = self.stats.entry_async(cache_key_hash).await.or_default();
        entry.miss_count += 1;
    }

    pub async fn get(&self, cache_key_hash: u64) -> QueryStats {
        self.stats
            .get_async(&cache_key_hash)
            .await
            .map(|entry| entry.get().clone())
            .unwrap_or_default()
    }

    pub async fn clear(&self) {
        self.stats.clear_async().await
    }

    pub async fn len(&self) -> usize {
        self.stats.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.stats.is_empty()
    }
}
