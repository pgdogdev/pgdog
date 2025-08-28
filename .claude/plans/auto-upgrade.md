# Auto-Upgrade Major Versions Feature Plan

## Overview
Enable seamless PostgreSQL major version upgrades from pre-logical-replication versions (< 9.4) to current versions (17+) with minimal risk and zero downtime.

## Problem Statement
- PostgreSQL versions before 9.4 lack logical replication support
- Traditional pg_upgrade requires downtime
- Cross-version replication tools (Slony, Bucardo) require invasive triggers
- Risk of application incompatibility with new versions

## Solution Architecture

### Components
```
[Application] → [PGDog] → [Alice: PG 9.3 Primary]
                    ↓            ↓
              [Mirror/CDC]  [Bob: PG 9.3 Replica]
                    ↓            ↓
              [Charles: PG 17] ← [COPY]
```

### Phases

#### Phase 1: Initial Setup
1. Insert PGDog between application and existing database (Alice)
2. Create read replica (Bob) using native PG streaming replication
3. Provision target database (Charles) with desired major version

#### Phase 2: Data Synchronization
1. Initial bulk copy from Bob to Charles using parallel COPY
2. Begin mirroring all DML from Alice to Charles via PGDog proxy
3. Track and log any errors/discrepancies

#### Phase 3: Validation & Convergence
1. Compare query results between Alice and Charles
2. Log error rates and incompatibilities
3. Re-sync specific tables if needed via additional COPY operations
4. Continue until error rate reaches zero

#### Phase 4: Cutover
1. Once validated, switch PGDog routing to Charles
2. Deprecate Alice and Bob
3. Optional: Keep Alice as fallback for configurable period

## Implementation Design

### Core Components

#### 1. Version-Aware Proxy (`version_proxy.rs`)
```rust
pub struct VersionProxy {
    source: Cluster,        // Alice (9.3)
    replica: Cluster,       // Bob (9.3 replica)
    target: Cluster,        // Charles (17)
    state: UpgradeState,
    validator: ResultValidator,
}

pub enum UpgradeState {
    Initializing,
    BulkCopying { progress: f32 },
    Mirroring { error_rate: f32 },
    Validated,
    CutoverReady,
    Complete,
}
```

#### 2. Adaptive Sync Manager (`adaptive_sync.rs`)
```rust
pub struct AdaptiveSync {
    // Tracks which tables need re-sync based on error rates
    problem_tables: HashSet<String>,
    
    pub async fn sync_table(&mut self, table: &str) -> Result<()> {
        // Use existing ParallelSyncManager
    }
    
    pub async fun resync_if_needed(&mut self) -> Result<()> {
        // Re-COPY tables with high error rates
    }
}
```

#### 3. Result Validator (`validator.rs`)
```rust
pub struct ResultValidator {
    metrics: ValidationMetrics,
    
    pub async fn validate_query_result(
        &mut self,
        query: &Query,
        source_result: &QueryResult,
        target_result: &QueryResult,
    ) -> ValidationResult {
        // Compare results, log differences
        // Track patterns of incompatibility
    }
}

pub struct ValidationMetrics {
    total_queries: u64,
    matching_results: u64,
    syntax_errors: u64,
    data_mismatches: u64,
    feature_incompatibilities: HashMap<String, u64>,
}
```

### Configuration
```toml
[[auto_upgrade]]
source_database = "production_9.3"
source_user = "app_user"
replica_database = "production_9.3_replica"  # Optional, auto-provisioned if not specified
target_database = "production_17"
target_user = "app_user"

[auto_upgrade.validation]
error_threshold = 0.001  # 0.1% error rate tolerance
validation_duration = "24h"  # Minimum validation period
compare_results = true

[auto_upgrade.sync]
parallel_workers = 4
copy_batch_size = 10000
resync_on_error = true
max_resync_attempts = 3
```

## Technical Approach

### Leveraging Existing PGDog Components

1. **Reuse `Publisher::data_sync()`** for initial COPY
   - Already handles parallel table copying
   - Binary COPY protocol for performance
   - Progress tracking built-in

2. **Extend `Mirror` for CDC**
   - Add DML-only filtering
   - Implement persistent queue for reliability
   - Add transaction ordering guarantees

3. **Reuse `QueryParser`** for version compatibility
   - Detect incompatible syntax
   - Optionally rewrite queries for compatibility

### New Components Required

1. **Version Compatibility Layer**
   ```rust
   pub trait VersionAdapter {
       fn adapt_query(&self, query: &Query, from: PgVersion, to: PgVersion) -> Result<Query>;
       fn is_compatible(&self, feature: &str, version: PgVersion) -> bool;
   }
   ```

2. **State Persistence**
   ```rust
   pub struct UpgradeState {
       // Persist to disk for resume after restart
       sync_progress: HashMap<String, TableSyncState>,
       validation_metrics: ValidationMetrics,
       cutover_ready: bool,
   }
   ```

3. **Admin API Extensions**
   ```sql
   -- New admin commands
   SHOW UPGRADE STATUS;
   SHOW UPGRADE ERRORS;
   FORCE RESYNC TABLE 'users';
   INITIATE CUTOVER;
   ROLLBACK UPGRADE;
   ```

## Risk Mitigation

### Safety Features
1. **Read-only validation period** - No writes to target during validation
2. **Automatic rollback** - Revert to source if error rate exceeds threshold
3. **Gradual cutover** - Route percentage of traffic gradually
4. **Parallel operation** - Keep source running until fully validated

### Monitoring & Observability
```rust
pub struct UpgradeMetrics {
    // Prometheus metrics
    sync_progress: GaugeVec,
    error_rate: GaugeVec,
    query_latency_comparison: HistogramVec,
    table_row_counts: GaugeVec,
}
```

## Implementation Phases

### Phase 1: MVP (4-6 weeks)
- Basic COPY from replica to target
- Simple query mirroring (no validation)
- Manual cutover

### Phase 2: Validation (4-6 weeks)
- Result comparison
- Error tracking and reporting
- Automatic re-sync of problem tables

### Phase 3: Production Ready (4-6 weeks)
- State persistence
- Admin API
- Monitoring and alerting
- Automated cutover

### Phase 4: Advanced Features (4-8 weeks)
- Query rewriting for compatibility
- Gradual traffic shifting
- Multi-tenant support
- Cloud provider integrations (RDS replica provisioning)

## Success Criteria

1. **Zero-downtime upgrades** from PG 9.x to 17
2. **< 0.1% error rate** during validation
3. **< 5% performance overhead** during migration
4. **Automatic recovery** from transient failures
5. **Complete rollback capability** until final cutover

## Open Questions

1. **DDL Handling**: How to handle schema changes during migration?
   - Option A: Block DDL during migration
   - Option B: Replay DDL to target (risky)
   - Option C: Queue DDL for after cutover

2. **Sequence Synchronization**: How to keep sequences in sync?
   - Option A: Periodic sequence value sync
   - Option B: Reserve sequence ranges
   - Option C: Reset sequences during cutover

3. **Extension Compatibility**: How to handle version-specific extensions?
   - Option A: Pre-migration compatibility check
   - Option B: Extension mapping table
   - Option C: Fail migration if incompatible

## Dependencies

### External
- PostgreSQL 9.x support in client libraries
- Binary COPY protocol compatibility across versions
- Network bandwidth for initial copy

### Internal
- Existing `Publisher::data_sync()` infrastructure
- Existing `Mirror` query replay system
- Query parser for DML detection

## Timeline Estimate

- **MVP**: 4-6 weeks
- **Production Ready**: 12-18 weeks total
- **Full Feature Set**: 20-26 weeks total

## Alternative Approaches Considered

1. **Trigger-based CDC**: Rejected due to source database modifications
2. **WAL Shipping**: Rejected due to version incompatibility
3. **Logical Decoding Plugin**: Rejected as not available pre-9.4
4. **Foreign Data Wrapper**: Rejected due to performance concerns

## Conclusion

This feature leverages PGDog's unique position as a transparent proxy to enable zero-downtime, low-risk major version upgrades without requiring logical replication support. By combining existing data sync and mirroring capabilities with new validation logic, we can provide a solution that no other tool currently offers for pre-9.4 PostgreSQL versions.