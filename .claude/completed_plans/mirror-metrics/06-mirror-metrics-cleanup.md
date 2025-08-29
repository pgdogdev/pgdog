# Sub-Plan 06: Mirror Metrics Cleanup

**STATUS: 100% COMPLETE - Ready to move to completed_plans**
**Created: 2025-08-29**
**Last Updated: 2025-08-29 by Plan Detective**
**Priority: HIGH**

**Detective Investigation Results:**
- ✅ **Prometheus naming** - COMPLETE via `openmetrics_namespace = "pgdog_"` in config
- ✅ **parking_lot::RwLock** - ALREADY IMPLEMENTED (no std::sync::RwLock found)
- ✅ **Database/user context** - ALREADY IMPLEMENTED (handler has fields)
- ✅ **Log capitalization** - ALREADY CORRECT (all lowercase)

## Objective
Clean up and standardize the mirror metrics implementation to follow Prometheus best practices, improve reliability, and enhance consistency.

## Tasks

### 1. Metric Name Migration to Prometheus Standards
Migrate metric names to adhere to [Prometheus naming conventions](https://prometheus.io/docs/practices/naming/).

**Current → New metric names:**
```
mirror_requests_total → pgdog_mirror_requests_total
mirror_requests_mirrored → pgdog_mirror_requests_mirrored_total  
mirror_requests_dropped → pgdog_mirror_requests_dropped_total
mirror_errors_connection → pgdog_mirror_errors_connection_total
mirror_errors_query → pgdog_mirror_errors_query_total
mirror_errors_timeout → pgdog_mirror_errors_timeout_total
mirror_errors_buffer_full → pgdog_mirror_errors_buffer_full_total
mirror_latency_avg_ms → pgdog_mirror_latency_milliseconds_avg
mirror_latency_max_ms → pgdog_mirror_latency_milliseconds_max
mirror_consecutive_errors → pgdog_mirror_consecutive_errors_current
mirror_last_success_seconds_ago → pgdog_mirror_last_success_seconds
```

**Key changes:**
- Add `pgdog_` prefix for namespace
- Add `_total` suffix for counters
- Use base units (milliseconds → include unit in name)
- Remove redundant words like "ago"

**Files to modify:**
- `pgdog/src/stats/mirror.rs` - Update OpenMetric implementation

### 2. Replace std::RwLock with parking_lot::RwLock
Replace `std::sync::RwLock` with `parking_lot::RwLock` to avoid poisoning and panics.

**Benefits:**
- No poisoning on panic
- Better performance
- Simpler error handling (no `unwrap()` needed)

**Changes needed:**
```rust
// Before
use std::sync::RwLock;
pub last_success: RwLock<Instant>,
*self.last_success.write().unwrap() = Instant::now();

// After  
use parking_lot::RwLock;
pub last_success: RwLock<Instant>,
*self.last_success.write() = Instant::now();
```

**Files to modify:**
- `pgdog/src/stats/mirror.rs` - Update all RwLock usage
- `pgdog/Cargo.toml` - Ensure parking_lot dependency exists

### 3. Add Database/User Context to Dropped Requests
Currently, dropped requests are tracked without database/user context. Fix this by passing context to MirrorHandler.

**Approach:**
- Add database and user references to MirrorHandler
- Update `record_error()` calls for BufferFull to include proper context
- Consider storing a reference to the Mirror struct itself for full context

**Current code to fix:**
```rust
// pgdog/src/backend/pool/connection/mirror/handler.rs
stats.record_error("unknown", "unknown", MirrorErrorType::BufferFull);
```

**Files to modify:**
- `pgdog/src/backend/pool/connection/mirror/handler.rs` - Add context fields
- `pgdog/src/backend/pool/connection/mirror/mod.rs` - Pass context when creating handler

### 4. Standardize Log Message Capitalization
Ensure all log messages start with lowercase for consistency.

**Examples to fix:**
```rust
// Before
error!("Mirror error: {}", err);
debug!("Mirror dropping request");

// After  
error!("mirror error: {}", err);
debug!("mirror dropping request");
```

**Search patterns:**
- `error!("M` → `error!("m`
- `warn!("M` → `warn!("m`
- `info!("M` → `info!("m`
- `debug!("M` → `debug!("m`
- `trace!("M` → `trace!("m`

**Files to check:**
- `pgdog/src/backend/pool/connection/mirror/mod.rs`
- `pgdog/src/backend/pool/connection/mirror/handler.rs`
- Any other files with mirror-related logging

## Testing Plan

1. **Metric name changes:**
   - Update integration test to use new metric names
   - Verify metrics endpoint outputs correct names
   - Test with actual Prometheus scraper if available

2. **RwLock migration:**
   - Run existing tests to ensure no regressions
   - Verify no panics under concurrent load

3. **Dropped request context:**
   - Add test to verify database/user are properly recorded for dropped requests
   - Check SHOW MIRROR_STATS displays per-database dropped counts

4. **Log consistency:**
   - Manual review of log output
   - Grep for any remaining capitalized log messages

## Success Criteria

- [ ] All metrics follow Prometheus naming conventions with `pgdog_` prefix
- [ ] No std::sync::RwLock usage remains in mirror stats
- [ ] Dropped requests include proper database/user context
- [ ] All log messages start with lowercase
- [ ] All existing tests still pass
- [ ] Integration test updated for new metric names

## Estimated Effort
- 2-3 hours for implementation
- 1 hour for testing and verification