# Mirror Stats Bug Fix Plan - Phase 2B

**Created: 2025-08-29**  
**Status: READY FOR IMPLEMENTATION**  
**Priority: CRITICAL** (Incorrect metrics give false confidence about mirroring health)
**Root Cause: IDENTIFIED** (DevNull stream hides connection failures)

## Executive Summary

Mirror requests to unreachable destinations are incorrectly counted as "mirrored" (successful) instead of errors. This gives users false confidence that mirroring is working when it's actually failing silently.

## Bug Details

### Current Behavior (INCORRECT)
When mirroring to an unreachable destination (e.g., banned pool):
- `requests_total`: Incremented correctly ✅
- `requests_mirrored`: Incremented (1 out of 3) ❌ **BUG**
- `requests_dropped`: 0 (correct if queue accepts it)
- `errors_connection`: 0 ❌ Should be non-zero

### Expected Behavior
When mirror destination is unreachable:
- `requests_total`: Should increment ✅
- `requests_mirrored`: Should remain 0 (no successful mirrors)
- `requests_dropped`: 0 (unless queue overflow)
- `errors_connection`: Should increment for each failed attempt

### Test Evidence
```
=== Mirror Stats After Transaction ===
Total requests: 3
Mirrored (claimed successful): 1  ❌ BUG
Dropped: 0
Connection errors: 0  ❌ Should be > 0
```

## Root Cause Analysis

### 1. Stream::DevNull Always Succeeds
The mirror uses `Stream::DevNull` which:
- Always returns `Ok(buf.len())` for writes
- Always returns `Ok(())` for flush/shutdown
- Never actually attempts network operations

```rust
// In Mirror::new()
stream: Stream::DevNull,  // This never fails!
```

### 2. Query Engine Execution Path
When processing mirror requests:
1. `Mirror::handle()` creates a `QueryEngineContext` with the DevNull stream
2. `query_engine.handle(&mut context)` processes the request
3. The query engine routes the transaction but doesn't check pool availability for mirrors
4. `backend.handle_client_request()` would normally get a connection, but with DevNull it skips this
5. Since the stream is DevNull, all writes "succeed" without actually connecting
6. The function returns `Ok(())` even though no real mirroring occurred
7. `record_success()` is called instead of `record_error()`

### 3. Missing Connection Validation
The mirror never actually attempts to:
- Get a connection from the pool
- Check if the pool is banned
- Validate the destination is reachable

### 4. Partial Success Mystery
Why only 1 out of 3 requests marked as mirrored:
- The mirror handler uses states: `Idle`, `Sending`, `Dropping`
- First query (BEGIN) might succeed in being queued
- Subsequent queries might be dropped due to exposure sampling or state transitions
- This needs further investigation but indicates inconsistent behavior

## Proposed Solution

### Approach A: Early Connection Validation (Recommended)
Check pool availability before processing mirror requests:

1. **Add pool reference to Mirror struct**
   - Store reference to destination cluster/pool
   - Check pool status before processing

2. **Validate connection availability**
   - Before processing each batch, check if pool is banned
   - If banned, record as connection error
   - Only proceed if pool is healthy

3. **Benefits**
   - Fail fast without wasting resources
   - Clear error categorization
   - Minimal changes to existing flow

### Approach B: Real Connection Attempt
Actually attempt to connect and execute queries:

1. **Replace DevNull with real connection**
   - Get actual connection from pool
   - Execute queries against mirror destination
   - Handle connection failures properly

2. **Challenges**
   - Major refactoring required
   - Performance implications
   - Resource usage for mirror connections

### Approach C: Hybrid - Lazy Connection
Check pool status but keep DevNull for execution:

1. **Add pool status check**
   - Verify pool is not banned
   - Check connection availability
   - Use DevNull for actual execution (performance)

2. **Benefits**
   - Accurate error detection
   - Maintains performance characteristics
   - Minimal resource usage

## Implementation Plan

### Phase 1: Add Pool Reference to Mirror
**File:** `pgdog/src/backend/pool/connection/mirror/mod.rs`

```rust
pub struct Mirror {
    // ... existing fields ...
    /// Reference to destination cluster for health checks
    pub cluster: Arc<Cluster>,
}
```

### Phase 2: Check Pool Status Before Processing
**File:** `pgdog/src/backend/pool/connection/mirror/mod.rs`

```rust
impl Mirror {
    pub async fn handle(
        &mut self,
        request: &mut MirrorRequest,
        query_engine: &mut QueryEngine,
    ) -> Result<(), Error> {
        // Check if destination pool is available
        if self.cluster.pool().is_banned() {
            return Err(Error::Pool(PoolError::Banned));
        }
        
        // Continue with existing logic...
        debug!("mirroring {} client requests", request.buffer.len());
        // ...
    }
}
```

### Phase 3: Update Stats Recording
**File:** `pgdog/src/backend/pool/connection/mirror/mod.rs`

Ensure connection errors are properly categorized:
```rust
match mirror.handle(&mut req, &mut query_engine).await {
    Ok(_) => {
        let latency_ms = start.elapsed().as_millis() as u64;
        MirrorStats::instance().record_success(&database_name, &user_name, latency_ms);
    }
    Err(err) => {
        let error_type = categorize_mirror_error(&err);
        MirrorStats::instance().record_error(&database_name, &user_name, error_type);
        // Don't increment mirrored counter!
        error!("mirror error: {} (type: {:?})", err, error_type);
    }
}
```

### Phase 4: Fix the Spawn Flow
The mirror needs access to the cluster/pool to check status:

1. Pass cluster reference through spawn methods
2. Store it in the Mirror struct
3. Use it for health checks

## Testing Strategy

### 1. Unit Tests
- Test pool ban detection
- Test error categorization
- Test stats increment logic

### 2. Integration Test Updates
**File:** `integration/rust/tests/integration/test_mirror_stats_bug.rs`

Update assertions to expect correct behavior:
```rust
// After fix, should have:
assert_eq!(stats_mirrored, 0, "No requests should be mirrored to unreachable destination");
assert!(errors_connection > 0, "Should have connection errors for banned pool");
```

### 3. Existing Test Updates
- `test_mirror_queue_overflow` - Update expectations
- Add new test for various failure scenarios

## Success Criteria

1. **No false positives:** Requests to unreachable destinations are never counted as "mirrored"
2. **Accurate error tracking:** Connection failures properly counted in `errors_connection`
3. **Queue behavior unchanged:** Queue overflow still counts as "dropped"
4. **Performance maintained:** Minimal overhead for health checks

## Risks and Mitigations

### Risk 1: Performance Impact
- **Risk:** Checking pool status adds latency
- **Mitigation:** Cache pool status, check only once per batch

### Risk 2: Breaking Changes
- **Risk:** Changing Mirror struct breaks compatibility
- **Mitigation:** Careful refactoring, maintain existing interfaces

### Risk 3: Incomplete Error Coverage
- **Risk:** Other error types not properly categorized
- **Mitigation:** Comprehensive error type mapping, extensive testing

## Implementation Order

1. **First:** Create comprehensive test suite (30 min)
2. **Second:** Implement pool status checking (1 hour)
3. **Third:** Fix stats recording logic (30 min)
4. **Fourth:** Update all affected tests (30 min)
5. **Fifth:** Performance testing (30 min)

## Notes

- This is a critical bug affecting observability
- Users may have been relying on incorrect metrics
- Consider adding a migration note or warning
- May want to add debug logging for mirror attempts

## Decision Log

1. **Stream::DevNull vs Real Connection:** Keeping DevNull for performance, adding health checks
2. **Error Classification:** Connection errors for banned pools, not "dropped"
3. **Backwards Compatibility:** Maintaining existing interfaces where possible

## Key Insights from Investigation

1. **DevNull Stream Hides Failures:** The use of `Stream::DevNull` means all network operations appear successful even when the destination is unreachable. This is the core issue.

2. **No Pool Interaction:** The mirror never attempts to get a connection from the pool, so it never discovers the pool is banned.

3. **Inconsistent State Machine:** The mirror handler's state machine (Idle/Sending/Dropping) leads to inconsistent behavior where only some requests are counted as mirrored.

4. **Missing Error Path:** There's no code path that checks pool health before processing mirror requests, so errors are never detected or recorded.