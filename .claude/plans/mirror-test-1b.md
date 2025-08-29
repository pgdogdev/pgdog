# Test 1B: Statistical Distribution Test for Mirroring with Sharding

**Created:** 2025-08-29  
**Status:** PLANNED  
**Priority:** HIGH  
**Effort:** 4 hours

## Overview

This test validates that mirroring with partial exposure (0.5) correctly applies both statistical sampling AND sharding logic. It ensures that:
1. Only ~50% of queries are mirrored (exposure=0.5)
2. The mirrored subset is still correctly sharded
3. MirrorStats accurately tracks total/mirrored/dropped counts

## Key Differences from Test 1A

| Aspect | Test 1A (Existing) | Test 1B (Planned) |
|--------|-------------------|-------------------|
| Exposure | 1.0 (deterministic) | 0.5 (probabilistic) |
| Row Count | 4 rows | 100 rows |
| Validation | Exact counts | Statistical ranges |
| Focus | Sharding correctness | Exposure sampling + sharding |
| MirrorStats | Not checked | Explicitly validated |

## Implementation Design

### Test Function Name
`test_mirroring_statistical_distribution_with_sharding`

### Configuration Changes
```toml
[[mirroring]]
source = "source_db"
destination = "sharded_cluster"
exposure = 0.5  # Key difference: only 50% of queries mirrored
```

### Test Setup
1. **Identical to Test 1A for infrastructure:**
   - Source database: `source_db`
   - Sharded cluster: `sharded_cluster` (shard_0, shard_1)
   - Sharding on `user_id` column (even→shard_0, odd→shard_1)

2. **Key additions:**
   - Import MirrorStats: `use pgdog::stats::mirror::MirrorStats;`
   - Reset stats before test: `MirrorStats::instance().reset_counters();`
   - Larger dataset: 100 sequential user_ids

### Core Test Logic

```rust
// Insert 100 rows with sequential user_ids
for i in 1..=100 {
    client.simple_query("BEGIN").await.unwrap();
    let query = format!(
        "INSERT INTO users (user_id, name) VALUES ({}, 'User {}')",
        i, i
    );
    client.simple_query(&query).await.unwrap();
    client.simple_query("COMMIT").await.unwrap();
    
    // Small delay to avoid overwhelming the mirror queue
    if i % 10 == 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// Wait longer for mirror processing due to larger dataset
tokio::time::sleep(Duration::from_millis(2000)).await;
```

### Validation Strategy

#### 1. Source Database Validation
```rust
let source_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
    .fetch_one(&mut source_conn)
    .await
    .expect("Failed to query source");

assert_eq!(source_count.0, 100, "Source should have exactly 100 rows");
```

#### 2. Mirror Distribution Validation
```rust
let shard0_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
    .fetch_one(&mut shard0_conn)
    .await
    .expect("Failed to query shard_0");

let shard1_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
    .fetch_one(&mut shard1_conn)
    .await
    .expect("Failed to query shard_1");

let total_mirrored = shard0_count.0 + shard1_count.0;

// Statistical assertion with tolerance
assert!(
    total_mirrored >= 35 && total_mirrored <= 65,
    "Expected ~50 rows mirrored (±15), got {}",
    total_mirrored
);

// Verify roughly even distribution between shards (with wider tolerance)
let shard_diff = (shard0_count.0 - shard1_count.0).abs();
let max_acceptable_diff = (total_mirrored as f64 * 0.4) as i64;
assert!(
    shard_diff <= max_acceptable_diff,
    "Shard distribution too uneven: shard0={}, shard1={}, diff={}",
    shard0_count.0, shard1_count.0, shard_diff
);
```

#### 3. Sharding Correctness Validation
```rust
// Verify that mirrored rows still respect sharding rules
let shard0_users: Vec<(i64,)> = sqlx::query_as("SELECT user_id FROM users")
    .fetch_all(&mut shard0_conn)
    .await
    .expect("Failed to query shard_0 users");

let shard1_users: Vec<(i64,)> = sqlx::query_as("SELECT user_id FROM users")
    .fetch_all(&mut shard1_conn)
    .await
    .expect("Failed to query shard_1 users");

// All shard0 users should have even IDs
for (user_id,) in &shard0_users {
    assert_eq!(
        user_id % 2, 0,
        "Shard 0 should only contain even user_ids, found {}",
        user_id
    );
}

// All shard1 users should have odd IDs
for (user_id,) in &shard1_users {
    assert_eq!(
        user_id % 2, 1,
        "Shard 1 should only contain odd user_ids, found {}",
        user_id
    );
}

// Verify no duplicate user_ids across shards
let mut all_ids = HashSet::new();
for (user_id,) in shard0_users.iter().chain(shard1_users.iter()) {
    assert!(
        all_ids.insert(*user_id),
        "Duplicate user_id {} found across shards",
        user_id
    );
}
```

#### 4. MirrorStats Validation
```rust
// Access MirrorStats instance
let mirror_stats = MirrorStats::instance();

// Check global counters
let stats_total = mirror_stats.requests_total.load(Ordering::Relaxed);
let stats_mirrored = mirror_stats.requests_mirrored.load(Ordering::Relaxed);
let stats_dropped = mirror_stats.requests_dropped.load(Ordering::Relaxed);

println!("MirrorStats - Total: {}, Mirrored: {}, Dropped: {}", 
         stats_total, stats_mirrored, stats_dropped);

// Validate stats match our observations
assert_eq!(
    stats_total, 100,
    "MirrorStats should show 100 total requests"
);

assert_eq!(
    stats_mirrored, total_mirrored as u64,
    "MirrorStats mirrored count should match actual mirrored rows"
);

assert_eq!(
    stats_dropped, (100 - total_mirrored) as u64,
    "MirrorStats dropped count should be (total - mirrored)"
);

assert_eq!(
    stats_mirrored + stats_dropped, stats_total,
    "Mirrored + Dropped should equal Total"
);
```

## Edge Cases to Consider

1. **Mirror Queue Overflow**: With 100 rapid inserts, ensure queue doesn't overflow
   - Solution: Add small delays every 10 inserts
   
2. **Timing Issues**: Mirror processing might not complete before validation
   - Solution: Increase wait time to 2000ms
   
3. **Statistical Variance**: Random sampling means results vary between runs
   - Solution: Use reasonable tolerance ranges (±15% for total, ±40% for distribution)

4. **Stats Reset**: Other tests might pollute MirrorStats
   - Solution: Reset counters at test start
   - Consider: Run with `--test-threads=1`

## Success Criteria

✅ Test passes consistently (accounting for statistical variance)  
✅ Exposure sampling works (~50% mirrored with ±15% tolerance)  
✅ Sharding rules still apply to mirrored subset  
✅ No rows in wrong shards (even in shard0, odd in shard1)  
✅ No duplicate rows across shards  
✅ MirrorStats accurately reflects operations  

## Dependencies

- Requires MirrorStats to be accessible from tests (might need `pub` visibility)
- Assumes mirror queue depth is sufficient for 100 operations
- Requires PostgreSQL test databases to be available

## Future Enhancements

1. **Test different exposure values**: 0.1, 0.25, 0.75, 0.9
2. **Test with multiple mirror destinations**: Different exposures per destination
3. **Performance metrics**: Measure latency impact of mirroring
4. **Failure scenarios**: Test with one shard unavailable
5. **Queue overflow testing**: Explicitly test queue_depth limits

## Implementation Notes

- Place in same file as Test 1A: `integration/rust/tests/integration/test_mirror_with_sharding.rs`
- Use `#[tokio::test]` and `#[serial]` attributes
- Consider extracting common setup code into helper functions
- Add detailed logging for debugging statistical failures

## Estimated Timeline

1. **Initial implementation**: 2 hours
2. **Statistical tuning**: 1 hour (finding right tolerances)
3. **MirrorStats integration**: 30 minutes
4. **Testing and debugging**: 30 minutes

**Total**: ~4 hours