# PgDog Mirror Metrics Development Plan

## Executive Summary

PgDog's database mirroring feature enables risk-free migration testing by asynchronously replicating queries from a source cluster to a target cluster. However, the lack of metrics visibility creates operational blind spots, preventing teams from confidently determining when a zero-downtime migration can be safely executed. This development plan outlines the implementation of comprehensive mirror metrics that will provide real-time visibility into mirror performance, error rates, and migration readiness.

### Business Impact

- **Risk Reduction**: Quantifiable metrics enable data-driven migration decisions
- **Operational Excellence**: Proactive monitoring prevents migration failures
- **Customer Confidence**: Clear success criteria for production cutover
- **Time to Market**: Faster migration validation through automated readiness assessment

## Current State Analysis

### Existing Implementation

The mirror functionality is implemented across several modules:

#### Core Components

1. **Mirror Module** (`pgdog/src/backend/pool/connection/mirror/mod.rs`)
   - Line 103: Errors logged but not tracked: `error!("mirror error: {}", err);`
   - No success/failure counters
   - No performance tracking
   - Exposure controlled via `mirror_exposure` (0.0-1.0)

2. **Configuration** (`pgdog/src/config/mod.rs`)
   - `mirror_of`: Target database for mirroring
   - `mirror_exposure`: Percentage of traffic to mirror (0.0-1.0)
   - `mirror_queue`: Buffer size for async operations (default: 100)

3. **Mirror Handler** (`pgdog/src/backend/pool/connection/mirror/handler.rs`)
   - Manages async request buffering
   - Random sampling based on exposure percentage
   - No metrics collection points

### Architecture Gaps

- **No Statistical Tracking**: Success/failure rates unavailable
- **Limited Error Visibility**: Errors logged to text logs only
- **No Performance Metrics**: Latency and throughput unmeasured
- **Missing Health Indicators**: No way to assess mirror cluster health
- **Lack of Alerting**: No mechanism for threshold-based notifications

## Detailed Implementation Plan

### Phase 1: Core Metrics Infrastructure (Week 1)

#### 1.1 Create MirrorStats Structure

**Location**: `pgdog/src/stats/mirror.rs` (new file)

```rust
pub struct MirrorStats {
    // Request counters
    pub requests_total: AtomicU64,
    pub requests_mirrored: AtomicU64,
    pub requests_dropped: AtomicU64,
    
    // Error tracking
    pub errors_connection: AtomicU64,
    pub errors_query: AtomicU64,
    pub errors_timeout: AtomicU64,
    pub errors_buffer_full: AtomicU64,
    
    // Performance metrics
    pub latency_sum_ms: AtomicU64,
    pub latency_count: AtomicU64,
    pub latency_max_ms: AtomicU64,
    
    // Health indicators
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

#### 1.2 Singleton Pattern Implementation

**Location**: `pgdog/src/stats/mirror.rs`

```rust
static MIRROR_STATS: OnceLock<Arc<MirrorStats>> = OnceLock::new();

impl MirrorStats {
    pub fn instance() -> Arc<MirrorStats> {
        MIRROR_STATS.get_or_init(|| {
            Arc::new(MirrorStats::default())
        }).clone()
    }
    
    pub fn record_success(&self, database: &str, latency_ms: u64) {
        self.requests_mirrored.fetch_add(1, Ordering::Relaxed);
        self.latency_sum_ms.fetch_add(latency_ms, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        
        // Update max latency if needed
        let mut current_max = self.latency_max_ms.load(Ordering::Relaxed);
        while latency_ms > current_max {
            match self.latency_max_ms.compare_exchange_weak(
                current_max,
                latency_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
        
        // Update database-specific stats
        self.database_stats
            .entry(database.to_string())
            .or_insert_with(DatabaseMirrorStats::default)
            .mirrored.fetch_add(1, Ordering::Relaxed);
        
        // Reset consecutive errors
        self.consecutive_errors.store(0, Ordering::Relaxed);
        *self.last_success.write() = Instant::now();
    }
    
    pub fn record_error(&self, database: &str, error_type: MirrorErrorType) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        
        match error_type {
            MirrorErrorType::Connection => {
                self.errors_connection.fetch_add(1, Ordering::Relaxed);
            }
            MirrorErrorType::Query => {
                self.errors_query.fetch_add(1, Ordering::Relaxed);
            }
            MirrorErrorType::Timeout => {
                self.errors_timeout.fetch_add(1, Ordering::Relaxed);
            }
            MirrorErrorType::BufferFull => {
                self.errors_buffer_full.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        self.database_stats
            .entry(database.to_string())
            .or_insert_with(DatabaseMirrorStats::default)
            .errors.fetch_add(1, Ordering::Relaxed);
        
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write() = Some(Instant::now());
    }
}
```

### Phase 2: Integration with Mirror Module (Week 1-2)

#### 2.1 Modify Mirror Handler

**Location**: `pgdog/src/backend/pool/connection/mirror/mod.rs`

Update lines 100-110 to include metrics collection:

```rust
// Before (line 103)
if let Err(err) = mirror.handle(&mut req, &mut query_engine).await {
    error!("mirror error: {}", err);
}

// After
let start = Instant::now();
match mirror.handle(&mut req, &mut query_engine).await {
    Ok(_) => {
        let latency_ms = start.elapsed().as_millis() as u64;
        MirrorStats::instance().record_success(&mirror.params.database, latency_ms);
        debug!("mirror request completed in {}ms", latency_ms);
    }
    Err(err) => {
        let error_type = categorize_error(&err);
        MirrorStats::instance().record_error(&mirror.params.database, error_type);
        error!("mirror error: {} (type: {:?})", err, error_type);
    }
}
```

#### 2.2 Add Metrics to Buffer Handler

**Location**: `pgdog/src/backend/pool/connection/mirror/handler.rs`

Track buffer drops and exposure sampling:

```rust
impl MirrorHandler {
    pub fn send(&mut self, req: ClientRequest) -> bool {
        let stats = MirrorStats::instance();
        stats.requests_total.fetch_add(1, Ordering::Relaxed);
        
        // Check exposure
        if !self.should_mirror() {
            return true; // Not selected for mirroring
        }
        
        // Try to send
        match self.tx.try_send(MirrorRequest::new(req)) {
            Ok(_) => true,
            Err(TrySendError::Full(_)) => {
                stats.record_error("unknown", MirrorErrorType::BufferFull);
                stats.requests_dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(TrySendError::Closed(_)) => {
                error!("mirror channel closed");
                false
            }
        }
    }
}
```

### Phase 3: OpenMetrics Implementation (Week 2)

#### 3.1 Implement OpenMetric Trait

**Location**: `pgdog/src/stats/mirror.rs`

```rust
impl OpenMetric for MirrorStats {
    fn name(&self) -> String {
        "mirror".to_string()
    }
    
    fn measurements(&self) -> Vec<Measurement> {
        let mut measurements = vec![];
        
        // Request metrics
        measurements.push(Measurement {
            labels: vec![("type".into(), "total".into())],
            measurement: self.requests_total.load(Ordering::Relaxed).into(),
        });
        
        measurements.push(Measurement {
            labels: vec![("type".into(), "mirrored".into())],
            measurement: self.requests_mirrored.load(Ordering::Relaxed).into(),
        });
        
        measurements.push(Measurement {
            labels: vec![("type".into(), "dropped".into())],
            measurement: self.requests_dropped.load(Ordering::Relaxed).into(),
        });
        
        // Error metrics by type
        measurements.push(Measurement {
            labels: vec![("error_type".into(), "connection".into())],
            measurement: self.errors_connection.load(Ordering::Relaxed).into(),
        });
        
        measurements.push(Measurement {
            labels: vec![("error_type".into(), "query".into())],
            measurement: self.errors_query.load(Ordering::Relaxed).into(),
        });
        
        measurements.push(Measurement {
            labels: vec![("error_type".into(), "timeout".into())],
            measurement: self.errors_timeout.load(Ordering::Relaxed).into(),
        });
        
        measurements.push(Measurement {
            labels: vec![("error_type".into(), "buffer_full".into())],
            measurement: self.errors_buffer_full.load(Ordering::Relaxed).into(),
        });
        
        // Latency metrics
        let count = self.latency_count.load(Ordering::Relaxed);
        if count > 0 {
            let sum = self.latency_sum_ms.load(Ordering::Relaxed);
            let avg = sum as f64 / count as f64;
            
            measurements.push(Measurement {
                labels: vec![("stat".into(), "avg_latency_ms".into())],
                measurement: avg.into(),
            });
            
            measurements.push(Measurement {
                labels: vec![("stat".into(), "max_latency_ms".into())],
                measurement: self.latency_max_ms.load(Ordering::Relaxed).into(),
            });
        }
        
        // Health metrics
        measurements.push(Measurement {
            labels: vec![("health".into(), "consecutive_errors".into())],
            measurement: self.consecutive_errors.load(Ordering::Relaxed).into(),
        });
        
        let last_success_age = self.last_success.read().elapsed().as_secs();
        measurements.push(Measurement {
            labels: vec![("health".into(), "last_success_seconds_ago".into())],
            measurement: (last_success_age as i64).into(),
        });
        
        // Per-database metrics
        for entry in self.database_stats.iter() {
            let (database, stats) = entry.pair();
            
            measurements.push(Measurement {
                labels: vec![
                    ("database".into(), database.clone()),
                    ("metric".into(), "mirrored".into()),
                ],
                measurement: stats.mirrored.load(Ordering::Relaxed).into(),
            });
            
            measurements.push(Measurement {
                labels: vec![
                    ("database".into(), database.clone()),
                    ("metric".into(), "errors".into()),
                ],
                measurement: stats.errors.load(Ordering::Relaxed).into(),
            });
        }
        
        measurements
    }
    
    fn unit(&self) -> Option<String> {
        None
    }
    
    fn metric_type(&self) -> String {
        "gauge".to_string()
    }
    
    fn help(&self) -> Option<String> {
        Some("Mirror traffic statistics and health metrics".to_string())
    }
}
```

#### 3.2 Add to HTTP Metrics Endpoint

**Location**: `pgdog/src/stats/http_server.rs`

Update the metrics handler (lines 15-35):

```rust
async fn metrics(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let clients = Clients::load();
    let pools = Pools::load();
    let mirror = Metric::new(MirrorStats::instance().as_ref().clone());  // Add this
    let query_cache: Vec<_> = QueryCache::load()
        .metrics()
        .into_iter()
        .map(|m| m.to_string())
        .collect();
    let query_cache = query_cache.join("\n");
    
    // Include mirror metrics in output
    let metrics_data = format!(
        "{}\n{}\n{}\n{}", 
        clients, 
        pools, 
        mirror,  // Add mirror metrics
        query_cache
    );
    
    let response = Response::builder()
        .header(
            hyper::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Full::new(Bytes::from(metrics_data)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from("Metrics unavailable"))));

    Ok(response)
}
```

### Phase 4: Configuration Extensions (Week 2-3)

#### 4.1 Add Configuration Options

**Location**: `pgdog/src/config/mod.rs`

Add to General struct (around line 440):

```rust
/// Enable mirror metrics collection
#[serde(default = "General::mirror_metrics_enabled")]
pub mirror_metrics_enabled: bool,

/// Mirror error threshold for alerting (consecutive errors)
#[serde(default = "General::mirror_error_threshold")]
pub mirror_error_threshold: u64,

/// Mirror latency threshold in milliseconds
#[serde(default = "General::mirror_latency_threshold_ms")]
pub mirror_latency_threshold_ms: u64,

/// Mirror metrics reset interval in seconds
#[serde(default = "General::mirror_metrics_reset_interval")]
pub mirror_metrics_reset_interval: u64,
```

Add default implementations (around line 670):

```rust
fn mirror_metrics_enabled() -> bool {
    true
}

fn mirror_error_threshold() -> u64 {
    100  // Alert after 100 consecutive errors
}

fn mirror_latency_threshold_ms() -> u64 {
    5000  // Alert if latency exceeds 5 seconds
}

fn mirror_metrics_reset_interval() -> u64 {
    3600  // Reset counters every hour
}
```

### Phase 5: Migration Readiness Assessment (Week 3)

#### 5.1 Create Readiness Calculator

**Location**: `pgdog/src/stats/mirror_readiness.rs` (new file)

```rust
pub struct MigrationReadiness {
    pub ready: bool,
    pub confidence_score: f64,
    pub reasons: Vec<String>,
    pub metrics: ReadinessMetrics,
}

pub struct ReadinessMetrics {
    pub error_rate: f64,
    pub avg_latency_ms: f64,
    pub success_rate: f64,
    pub uptime_hours: f64,
    pub total_requests: u64,
    pub consecutive_successes: u64,
}

impl MirrorStats {
    pub fn calculate_readiness(&self, criteria: &ReadinessCriteria) -> MigrationReadiness {
        let total = self.requests_total.load(Ordering::Relaxed);
        let mirrored = self.requests_mirrored.load(Ordering::Relaxed);
        let errors = self.total_errors();
        
        let error_rate = if total > 0 {
            errors as f64 / total as f64
        } else {
            0.0
        };
        
        let success_rate = if total > 0 {
            (mirrored - errors) as f64 / total as f64
        } else {
            0.0
        };
        
        let avg_latency_ms = if self.latency_count.load(Ordering::Relaxed) > 0 {
            self.latency_sum_ms.load(Ordering::Relaxed) as f64 
                / self.latency_count.load(Ordering::Relaxed) as f64
        } else {
            0.0
        };
        
        let uptime_hours = self.last_success.read().elapsed().as_secs() as f64 / 3600.0;
        
        let mut ready = true;
        let mut reasons = vec![];
        let mut confidence_score = 100.0;
        
        // Check error rate
        if error_rate > criteria.max_error_rate {
            ready = false;
            confidence_score -= 30.0;
            reasons.push(format!(
                "Error rate {:.2}% exceeds threshold {:.2}%",
                error_rate * 100.0,
                criteria.max_error_rate * 100.0
            ));
        }
        
        // Check latency
        if avg_latency_ms > criteria.max_latency_ms {
            ready = false;
            confidence_score -= 20.0;
            reasons.push(format!(
                "Average latency {:.0}ms exceeds threshold {:.0}ms",
                avg_latency_ms,
                criteria.max_latency_ms
            ));
        }
        
        // Check minimum runtime
        if uptime_hours < criteria.min_runtime_hours {
            ready = false;
            confidence_score -= 25.0;
            reasons.push(format!(
                "Runtime {:.1} hours below minimum {} hours",
                uptime_hours,
                criteria.min_runtime_hours
            ));
        }
        
        // Check minimum requests
        if total < criteria.min_requests {
            ready = false;
            confidence_score -= 25.0;
            reasons.push(format!(
                "Total requests {} below minimum {}",
                total,
                criteria.min_requests
            ));
        }
        
        if ready {
            reasons.push("All migration criteria met".to_string());
        }
        
        MigrationReadiness {
            ready,
            confidence_score: confidence_score.max(0.0),
            reasons,
            metrics: ReadinessMetrics {
                error_rate,
                avg_latency_ms,
                success_rate,
                uptime_hours,
                total_requests: total,
                consecutive_successes: mirrored - errors,
            },
        }
    }
}
```

#### 5.2 Admin Command for Readiness Check

**Location**: `pgdog/src/admin/mirror_readiness.rs` (new file)

```rust
pub async fn show_mirror_readiness() -> Result<String, Error> {
    let stats = MirrorStats::instance();
    let criteria = ReadinessCriteria::from_config(&config().config.general);
    let readiness = stats.calculate_readiness(&criteria);
    
    let mut output = String::new();
    output.push_str("=== Mirror Migration Readiness Report ===\n\n");
    
    output.push_str(&format!("Status: {}\n", 
        if readiness.ready { "READY" } else { "NOT READY" }));
    output.push_str(&format!("Confidence Score: {:.1}%\n\n", readiness.confidence_score));
    
    output.push_str("Metrics:\n");
    output.push_str(&format!("  Error Rate: {:.3}%\n", readiness.metrics.error_rate * 100.0));
    output.push_str(&format!("  Success Rate: {:.3}%\n", readiness.metrics.success_rate * 100.0));
    output.push_str(&format!("  Avg Latency: {:.0}ms\n", readiness.metrics.avg_latency_ms));
    output.push_str(&format!("  Uptime: {:.1} hours\n", readiness.metrics.uptime_hours));
    output.push_str(&format!("  Total Requests: {}\n", readiness.metrics.total_requests));
    output.push_str(&format!("  Consecutive Successes: {}\n\n", 
        readiness.metrics.consecutive_successes));
    
    output.push_str("Assessment:\n");
    for reason in &readiness.reasons {
        output.push_str(&format!("  - {}\n", reason));
    }
    
    Ok(output)
}
```

### Phase 6: Testing Strategy (Week 3-4)

#### 6.1 Unit Tests

**Location**: `pgdog/src/stats/mirror.rs` (test module)

```rust
#[cfg(test)]
mod test {
    use super::*;
    
    #[test]
    fn test_mirror_stats_recording() {
        let stats = MirrorStats::default();
        
        // Record successes
        stats.record_success("test_db", 100);
        stats.record_success("test_db", 200);
        
        assert_eq!(stats.requests_mirrored.load(Ordering::Relaxed), 2);
        assert_eq!(stats.latency_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.latency_sum_ms.load(Ordering::Relaxed), 300);
        assert_eq!(stats.latency_max_ms.load(Ordering::Relaxed), 200);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 0);
    }
    
    #[test]
    fn test_error_tracking() {
        let stats = MirrorStats::default();
        
        stats.record_error("test_db", MirrorErrorType::Connection);
        stats.record_error("test_db", MirrorErrorType::Query);
        stats.record_error("test_db", MirrorErrorType::Timeout);
        
        assert_eq!(stats.errors_connection.load(Ordering::Relaxed), 1);
        assert_eq!(stats.errors_query.load(Ordering::Relaxed), 1);
        assert_eq!(stats.errors_timeout.load(Ordering::Relaxed), 1);
        assert_eq!(stats.consecutive_errors.load(Ordering::Relaxed), 3);
    }
    
    #[test]
    fn test_readiness_calculation() {
        let stats = MirrorStats::default();
        
        // Simulate traffic
        for _ in 0..1000 {
            stats.record_success("test_db", 50);
        }
        for _ in 0..10 {
            stats.record_error("test_db", MirrorErrorType::Query);
        }
        
        let criteria = ReadinessCriteria {
            max_error_rate: 0.02,  // 2%
            max_latency_ms: 100.0,
            min_runtime_hours: 0.0,  // For testing
            min_requests: 500,
        };
        
        let readiness = stats.calculate_readiness(&criteria);
        assert!(readiness.ready);
        assert!(readiness.metrics.error_rate < 0.02);
        assert_eq!(readiness.metrics.avg_latency_ms, 50.0);
    }
}
```

#### 6.2 Integration Tests

**Location**: `integration/mirror/metrics_spec.rb` (new file)

```ruby
require 'rspec'
require 'pg'

describe 'Mirror Metrics' do
  before(:all) do
    # Start PgDog with mirror configuration
    @pgdog = start_pgdog_with_mirror
    @primary = PG.connect(host: 'localhost', port: 6432, dbname: 'primary')
    @metrics_client = HTTPClient.new('http://localhost:9090')
  end
  
  it 'tracks successful mirror operations' do
    initial_metrics = fetch_mirror_metrics
    
    # Execute queries
    100.times do
      @primary.exec("SELECT 1")
    end
    
    sleep(1)  # Allow async processing
    
    final_metrics = fetch_mirror_metrics
    expect(final_metrics['mirror_requests_mirrored']).to be > initial_metrics['mirror_requests_mirrored']
    expect(final_metrics['mirror_errors_total']).to eq(initial_metrics['mirror_errors_total'])
  end
  
  it 'tracks mirror errors' do
    # Simulate mirror cluster failure
    stop_mirror_cluster
    
    initial_errors = fetch_mirror_metrics['mirror_errors_total']
    
    # Execute queries (should fail to mirror)
    10.times do
      @primary.exec("SELECT 1")
    end
    
    sleep(1)
    
    final_errors = fetch_mirror_metrics['mirror_errors_total']
    expect(final_errors).to be > initial_errors
  end
  
  it 'calculates migration readiness' do
    # Generate sufficient traffic
    1000.times do
      @primary.exec("SELECT 1")
    end
    
    readiness = @pgdog.admin_command("SHOW MIRROR READINESS")
    expect(readiness).to include("Status: READY")
    expect(readiness).to match(/Error Rate: \d+\.\d+%/)
    expect(readiness).to match(/Success Rate: \d+\.\d+%/)
  end
end
```

#### 6.3 Load Testing

**Location**: `integration/mirror/load_test.sh` (new file)

```bash
#!/bin/bash

# Load test for mirror metrics
echo "Starting mirror metrics load test..."

# Start PgDog with mirror configuration
./pgdog --config mirror_test.toml &
PGDOG_PID=$!

# Wait for startup
sleep 5

# Generate load
echo "Generating load..."
pgbench -h localhost -p 6432 -U pgdog -d primary \
        -c 10 -j 2 -t 10000 \
        -f custom_script.sql

# Check metrics endpoint
echo "Fetching metrics..."
curl -s http://localhost:9090/metrics | grep mirror_

# Check readiness
echo "Checking migration readiness..."
psql -h localhost -p 6433 -c "SHOW MIRROR READINESS"

# Cleanup
kill $PGDOG_PID
```

## Metric Definitions

### Counter Metrics

| Metric Name | Description | Labels | Unit |
|------------|-------------|---------|------|
| `mirror_requests_total` | Total requests received for mirroring | type | count |
| `mirror_requests_mirrored` | Successfully mirrored requests | type | count |
| `mirror_requests_dropped` | Requests dropped due to buffer overflow | type | count |
| `mirror_errors_connection` | Connection errors to mirror cluster | error_type | count |
| `mirror_errors_query` | Query execution errors on mirror | error_type | count |
| `mirror_errors_timeout` | Timeout errors during mirroring | error_type | count |
| `mirror_errors_buffer_full` | Buffer full errors | error_type | count |

### Gauge Metrics

| Metric Name | Description | Labels | Unit |
|------------|-------------|---------|------|
| `mirror_latency_avg_ms` | Average mirror operation latency | stat | milliseconds |
| `mirror_latency_max_ms` | Maximum mirror operation latency | stat | milliseconds |
| `mirror_consecutive_errors` | Current consecutive error count | health | count |
| `mirror_last_success_seconds_ago` | Time since last successful mirror | health | seconds |
| `mirror_buffer_utilization` | Current buffer usage percentage | resource | percent |

### Per-Database Metrics

| Metric Name | Description | Labels | Unit |
|------------|-------------|---------|------|
| `mirror_database_requests` | Requests per database | database, metric | count |
| `mirror_database_errors` | Errors per database | database, metric | count |
| `mirror_database_latency_ms` | Average latency per database | database, metric | milliseconds |

## Migration Readiness Criteria

### Default Thresholds

```toml
[general]
# Migration readiness thresholds
mirror_max_error_rate = 0.01        # 1% error rate
mirror_max_latency_ms = 1000        # 1 second
mirror_min_runtime_hours = 24       # 24 hours minimum
mirror_min_requests = 100000        # 100k requests minimum
mirror_consecutive_success = 10000  # 10k consecutive successes
```

### Readiness Algorithm

The migration readiness score is calculated based on:

1. **Error Rate** (30% weight)
   - Must be below configured threshold
   - Calculated as: `total_errors / total_requests`

2. **Latency** (20% weight)
   - Average latency must be below threshold
   - P99 latency should be < 2x average

3. **Runtime** (25% weight)
   - Mirror must run for minimum duration
   - Ensures sufficient observation period

4. **Volume** (25% weight)
   - Minimum request count ensures statistical significance
   - Prevents premature migration decisions

### Migration Decision Matrix

| Confidence Score | Recommendation | Action |
|-----------------|----------------|---------|
| 95-100% | Highly Confident | Safe to migrate immediately |
| 80-94% | Confident | Review specific issues, likely safe |
| 60-79% | Moderate | Address issues before migration |
| 40-59% | Low | Significant issues need resolution |
| 0-39% | Very Low | Do not migrate, major problems |

## Timeline and Milestones

### Week 1: Foundation
- [ ] Create MirrorStats structure
- [ ] Implement singleton pattern
- [ ] Add basic counter operations
- [ ] Write unit tests

### Week 2: Integration
- [ ] Integrate with mirror module
- [ ] Add OpenMetric implementation
- [ ] Update HTTP metrics endpoint
- [ ] Add configuration options

### Week 3: Advanced Features
- [ ] Implement readiness calculator
- [ ] Add admin commands
- [ ] Create integration tests
- [ ] Performance benchmarking

### Week 4: Polish and Documentation
- [ ] Load testing
- [ ] Documentation updates
- [ ] Prometheus dashboard templates
- [ ] Alert rule examples

## Success Criteria

### Technical Requirements

1. **Performance Impact**: < 1% overhead on mirror operations
2. **Memory Usage**: < 10MB for metrics storage
3. **Accuracy**: 100% accurate counting (atomic operations)
4. **Availability**: Metrics endpoint 99.9% available

### Business Requirements

1. **Visibility**: Complete visibility into mirror health
2. **Actionable**: Clear migration readiness assessment
3. **Alerting**: Proactive issue detection
4. **Confidence**: Data-driven migration decisions

### Quality Metrics

1. **Test Coverage**: > 90% for new code
2. **Documentation**: Complete API and user documentation
3. **Integration**: Seamless Prometheus/Grafana integration
4. **Backwards Compatibility**: No breaking changes

## Risk Mitigation

### Technical Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance degradation | High | Benchmark before/after, use atomics |
| Memory leaks | Medium | Bounded data structures, periodic reset |
| Metric accuracy | High | Extensive testing, atomic operations |
| Integration complexity | Medium | Phased rollout, feature flags |

### Operational Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Alert fatigue | Medium | Tunable thresholds, smart alerting |
| Misinterpretation | High | Clear documentation, training |
| False positives | Medium | Conservative defaults, validation |

## Appendix

### A. Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'pgdog_mirror'
    static_configs:
      - targets: ['pgdog:9090']
    scrape_interval: 15s
    metrics_path: /metrics
```

### B. Grafana Dashboard JSON

```json
{
  "dashboard": {
    "title": "PgDog Mirror Metrics",
    "panels": [
      {
        "title": "Mirror Success Rate",
        "targets": [
          {
            "expr": "rate(mirror_requests_mirrored[5m]) / rate(mirror_requests_total[5m])"
          }
        ]
      },
      {
        "title": "Mirror Latency",
        "targets": [
          {
            "expr": "mirror_latency_avg_ms"
          }
        ]
      },
      {
        "title": "Error Rate by Type",
        "targets": [
          {
            "expr": "rate(mirror_errors_connection[5m])"
          },
          {
            "expr": "rate(mirror_errors_query[5m])"
          },
          {
            "expr": "rate(mirror_errors_timeout[5m])"
          }
        ]
      }
    ]
  }
}
```

### C. Alert Rules

```yaml
groups:
  - name: mirror_alerts
    rules:
      - alert: HighMirrorErrorRate
        expr: rate(mirror_errors_total[5m]) > 0.01
        for: 5m
        annotations:
          summary: "High mirror error rate detected"
          description: "Mirror error rate is {{ $value }}%"
      
      - alert: MirrorLatencyHigh
        expr: mirror_latency_avg_ms > 1000
        for: 10m
        annotations:
          summary: "Mirror latency exceeds threshold"
          description: "Average latency is {{ $value }}ms"
      
      - alert: MirrorDown
        expr: mirror_last_success_seconds_ago > 300
        for: 1m
        annotations:
          summary: "Mirror appears to be down"
          description: "No successful mirrors for {{ $value }} seconds"
```

## Conclusion

This comprehensive plan provides a complete roadmap for implementing mirror failure tracking metrics in PgDog. The phased approach ensures minimal disruption while delivering immediate value through improved visibility and migration confidence. The implementation focuses on production-readiness with emphasis on performance, accuracy, and operational excellence.

Key deliverables include:
- Real-time mirror health visibility
- Automated migration readiness assessment  
- Prometheus-compatible metrics
- Comprehensive testing suite
- Production-ready alerting

Upon completion, teams will have full confidence in their migration decisions, backed by quantitative metrics and automated readiness assessments.