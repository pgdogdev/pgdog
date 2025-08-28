# Sub-Plan 01: Core Stats Structure

## Objective
Create the foundational `MirrorStats` structure with atomic counters and basic increment/record methods, establishing the data model for all mirror metrics.

## Dependencies
- None (this is the foundation)

## Failing Tests to Write First

```rust
// pgdog/src/stats/mirror.rs (test module)
#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_default_initialization() {
        let stats = MirrorStats::default();
        
        // All counters should start at 0
        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_dropped.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_connection.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_query.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_timeout.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors_buffer_full.load(Ordering::Relaxed), 0);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_increment_request_counters() {
        let stats = MirrorStats::default();
        
        stats.increment_total();
        stats.increment_total();
        stats.increment_mirrored();
        stats.increment_dropped();
        
        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 2);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 1);
        assert_eq!(stats.requests_dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_record_success_updates_counters() {
        let stats = MirrorStats::default();
        
        stats.record_success("test_db", 100);
        stats.record_success("test_db", 200);
        
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 2);
        assert_eq!(stats.latency_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.latency_sum_ms.load(Ordering::Relaxed), 300);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 200);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_success_updates_last_success_time() {
        let stats = MirrorStats::default();
        
        let before = Instant::now();
        std::thread::sleep(Duration::from_millis(10));
        stats.record_success("test_db", 50);
        let after = Instant::now();
        
        let last_success = *stats.last_success.read();
        assert!(last_success > before);
        assert!(last_success < after);
    }

    #[test]
    fn test_max_latency_updates_correctly() {
        let stats = MirrorStats::default();
        
        stats.record_success("db1", 100);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 100);
        
        stats.record_success("db2", 50);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 100); // Should not decrease
        
        stats.record_success("db3", 200);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 200); // Should update to new max
    }

    #[test]
    fn test_database_specific_stats() {
        let stats = MirrorStats::default();
        
        stats.record_success("db1", 100);
        stats.record_success("db1", 150);
        stats.record_success("db2", 200);
        
        let db1_stats = stats.database_stats.get("db1").unwrap();
        assert_eq!(db1_stats.mirrored.load(Ordering::Relaxed), 2);
        assert_eq!(db1_stats.errors.load(Ordering::Relaxed), 0);
        
        let db2_stats = stats.database_stats.get("db2").unwrap();
        assert_eq!(db2_stats.mirrored.load(Ordering::Relaxed), 1);
        assert_eq!(db2_stats.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_singleton_instance() {
        let instance1 = MirrorStats::instance();
        let instance2 = MirrorStats::instance();
        
        // Both should be the same instance
        instance1.increment_total();
        assert_eq!(instance2.requests_total.load(Ordering::Relaxed), 1);
        
        instance2.increment_total();
        assert_eq!(instance1.requests_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_concurrent_increments() {
        let stats = Arc::new(MirrorStats::default());
        let mut handles = vec![];
        
        for _ in 0..10 {
            let stats_clone = stats.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    stats_clone.increment_total();
                    stats_clone.record_success("test", 10);
                }
            }));
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 1000);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_reset_counters() {
        let stats = MirrorStats::default();
        
        stats.increment_total();
        stats.increment_mirrored();
        stats.record_success("test", 100);
        
        stats.reset_counters();
        
        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 0);
        assert_eq!(stats.latency_sum_ms.load(Ordering::Relaxed), 0);
        assert_eq!(stats.latency_count.load(Ordering::Relaxed), 0);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 0);
    }
}
```

## Implementation Steps

1. **Create the module file**
   - Create `pgdog/src/stats/mirror.rs`
   - Add `pub mod mirror;` to `pgdog/src/stats/mod.rs`

2. **Define the core structures**
   ```rust
   pub struct MirrorStats {
       // Request counters
       pub requests_total: AtomicU64,
       pub requests_mirrored: AtomicU64,
       pub requests_dropped: AtomicU64,
       
       // Error counters (will be used in sub-plan 02)
       pub errors_connection: AtomicU64,
       pub errors_query: AtomicU64,
       pub errors_timeout: AtomicU64,
       pub errors_buffer_full: AtomicU64,
       
       // Performance metrics
       pub latency_sum_ms: AtomicU64,
       pub latency_count: AtomicU64,
       pub latency_max_ms: AtomicU64,
       
       // Health tracking
       pub last_success: RwLock<Instant>,
       pub last_error: RwLock<Option<Instant>>,
       pub consecutive_errors: AtomicU64,
       
       // Per-database stats
       pub database_stats: DashMap<String, DatabaseMirrorStats>,
   }
   
   pub struct DatabaseMirrorStats {
       pub mirrored: AtomicU64,
       pub errors: AtomicU64,
       pub avg_latency_ms: AtomicU64,
   }
   ```

3. **Implement Default trait**
   ```rust
   impl Default for MirrorStats {
       fn default() -> Self {
           // Initialize all counters to 0
       }
   }
   ```

4. **Implement singleton pattern**
   ```rust
   static MIRROR_STATS: OnceLock<Arc<MirrorStats>> = OnceLock::new();
   
   impl MirrorStats {
       pub fn instance() -> Arc<MirrorStats> {
           MIRROR_STATS.get_or_init(|| {
               Arc::new(MirrorStats::default())
           }).clone()
       }
   }
   ```

5. **Implement basic increment methods**
   ```rust
   impl MirrorStats {
       pub fn increment_total(&self) {
           self.requests_total.fetch_add(1, Ordering::Relaxed);
       }
       
       pub fn increment_mirrored(&self) {
           self.requests_mirrored.fetch_add(1, Ordering::Relaxed);
       }
       
       pub fn increment_dropped(&self) {
           self.requests_dropped.fetch_add(1, Ordering::Relaxed);
       }
   }
   ```

6. **Implement record_success method**
   ```rust
   pub fn record_success(&self, database: &str, latency_ms: u64) {
       // Increment counters
       // Update latency metrics with atomic max operation
       // Update database-specific stats
       // Reset consecutive errors
       // Update last_success timestamp
   }
   ```

7. **Implement reset_counters method**
   ```rust
   pub fn reset_counters(&self) {
       // Reset all atomic counters to 0
       // Clear database_stats map
       // Keep timestamps as-is
   }
   ```

## Success Criteria

- [ ] All tests pass
- [ ] `cargo check` shows no warnings
- [ ] `cargo fmt` applied
- [ ] Atomic operations used correctly for thread safety
- [ ] DashMap properly used for concurrent database stats
- [ ] Singleton pattern correctly implemented with OnceLock
- [ ] Max latency updates correctly with compare_exchange_weak
- [ ] No memory leaks or race conditions in concurrent test

## Notes

- Focus only on the data structure and basic operations
- Error categorization logic will come in sub-plan 02
- No integration with actual mirror code yet
- No OpenMetrics implementation yet
- Keep it simple: just counters and basic recording methods