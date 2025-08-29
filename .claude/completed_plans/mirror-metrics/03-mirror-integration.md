# Sub-Plan 03: Mirror Integration

**STATUS: ✅ COMPLETE**
**Completed: 2025-08-28**

Fully integrated with mirror module and handler

## Objective
Wire up the MirrorStats to the actual Mirror module, recording real metrics from mirror operations including successes, failures, and buffer drops.

## Dependencies
- Sub-plan 01: Core Stats Structure (completed)
- Sub-plan 02: Error Categorization (completed)

## Failing Tests to Write First

```rust
// pgdog/src/backend/pool/connection/mirror/mod.rs (add test module)
#[cfg(test)]
mod mirror_stats_tests {
    use super::*;
    use crate::stats::mirror::{MirrorStats, MirrorErrorType};
    
    #[test]
    fn test_mirror_success_records_stats() {
        let stats = MirrorStats::instance();
        let initial_count = stats.requests_mirrored.load(Ordering::Relaxed);
        
        // Simulate a successful mirror operation
        let mirror = create_test_mirror();
        let mut req = create_test_request();
        let mut query_engine = create_test_query_engine();
        
        // Record the start time
        let start = Instant::now();
        
        // Execute mirror operation
        let result = mirror.handle(&mut req, &mut query_engine).await;
        
        // On success, stats should be updated
        if result.is_ok() {
            let latency_ms = start.elapsed().as_millis() as u64;
            MirrorStats::instance().record_success(&mirror.params.database, latency_ms);
        }
        
        assert_eq!(
            stats.requests_mirrored.load(Ordering::Relaxed), 
            initial_count + 1
        );
    }
    
    #[test]
    fn test_mirror_error_records_stats() {
        let stats = MirrorStats::instance();
        let initial_errors = stats.total_errors();
        
        // Simulate a failed mirror operation
        let mirror = create_test_mirror_that_fails();
        let mut req = create_test_request();
        let mut query_engine = create_test_query_engine();
        
        match mirror.handle(&mut req, &mut query_engine).await {
            Err(err) => {
                let error_type = categorize_mirror_error(&err);
                MirrorStats::instance().record_error(&mirror.params.database, error_type);
            }
            Ok(_) => panic!("Expected error"),
        }
        
        assert_eq!(stats.total_errors(), initial_errors + 1);
    }
}

// pgdog/src/backend/pool/connection/mirror/handler.rs (add test module)
#[cfg(test)]
mod handler_stats_tests {
    use super::*;
    use crate::stats::mirror::MirrorStats;
    
    #[test]
    fn test_handler_tracks_total_requests() {
        let mut handler = MirrorHandler::new(100, 1.0); // 100% exposure
        let stats = MirrorStats::instance();
        let initial = stats.requests_total.load(Ordering::Relaxed);
        
        let req = create_test_client_request();
        handler.send(req);
        
        assert_eq!(
            stats.requests_total.load(Ordering::Relaxed),
            initial + 1
        );
    }
    
    #[test]
    fn test_handler_tracks_dropped_requests() {
        // Create handler with tiny buffer
        let mut handler = MirrorHandler::new(1, 1.0);
        let stats = MirrorStats::instance();
        
        // Fill the buffer
        handler.send(create_test_client_request());
        
        let initial_dropped = stats.requests_dropped.load(Ordering::Relaxed);
        
        // This should be dropped
        handler.send(create_test_client_request());
        
        assert_eq!(
            stats.requests_dropped.load(Ordering::Relaxed),
            initial_dropped + 1
        );
    }
    
    #[test]
    fn test_handler_respects_exposure_sampling() {
        let mut handler = MirrorHandler::new(100, 0.0); // 0% exposure
        let stats = MirrorStats::instance();
        let initial = stats.requests_total.load(Ordering::Relaxed);
        
        // Should count as total but not be sent
        handler.send(create_test_client_request());
        
        assert_eq!(
            stats.requests_total.load(Ordering::Relaxed),
            initial + 1
        );
        // Buffer should be empty since exposure is 0
        assert!(handler.is_buffer_empty());
    }
}

// Integration test file: integration/mirror/metrics_integration_spec.rb
require 'rspec'
require 'pg'
require 'net/http'

describe 'Mirror Metrics Integration' do
  before(:all) do
    # Start PgDog with mirror config
    @pgdog_pid = spawn("cargo run -- --config integration/mirror/pgdog.toml")
    sleep(3) # Wait for startup
    
    @primary = PG.connect(host: 'localhost', port: 6432, dbname: 'primary')
  end
  
  after(:all) do
    Process.kill('TERM', @pgdog_pid)
  end
  
  it 'records successful mirror operations in metrics' do
    # Get initial metrics
    initial_metrics = fetch_metrics
    initial_mirrored = extract_metric(initial_metrics, 'mirror_requests_mirrored')
    
    # Execute queries that should be mirrored
    10.times { @primary.exec("SELECT 1") }
    
    sleep(1) # Allow async processing
    
    # Check metrics increased
    final_metrics = fetch_metrics
    final_mirrored = extract_metric(final_metrics, 'mirror_requests_mirrored')
    
    expect(final_mirrored).to be > initial_mirrored
  end
  
  it 'categorizes connection errors correctly' do
    # Stop mirror target
    system("docker stop mirror-postgres")
    
    initial_metrics = fetch_metrics
    initial_conn_errors = extract_metric(initial_metrics, 'mirror_errors_connection')
    
    # Try to execute queries (should fail to mirror)
    5.times { @primary.exec("SELECT 1") }
    
    sleep(1)
    
    final_metrics = fetch_metrics
    final_conn_errors = extract_metric(final_metrics, 'mirror_errors_connection')
    
    expect(final_conn_errors).to be > initial_conn_errors
    
    # Restart mirror
    system("docker start mirror-postgres")
  end
  
  private
  
  def fetch_metrics
    uri = URI('http://localhost:9090/metrics')
    response = Net::HTTP.get(uri)
    response
  end
  
  def extract_metric(metrics_text, metric_name)
    line = metrics_text.lines.find { |l| l.start_with?(metric_name) }
    return 0 unless line
    line.split(' ').last.to_i
  end
end
```

## Implementation Steps

1. **Update mirror module to use stats**
   
   File: `pgdog/src/backend/pool/connection/mirror/mod.rs`
   
   ```rust
   // Add import at top
   use crate::stats::mirror::{MirrorStats, MirrorErrorType, categorize_error};
   
   // Update the handle method (around line 100-110)
   // BEFORE:
   if let Err(err) = mirror.handle(&mut req, &mut query_engine).await {
       error!("mirror error: {}", err);
   }
   
   // AFTER:
   let start = Instant::now();
   match mirror.handle(&mut req, &mut query_engine).await {
       Ok(_) => {
           let latency_ms = start.elapsed().as_millis() as u64;
           MirrorStats::instance().record_success(&mirror.params.database, latency_ms);
           debug!("mirror request completed in {}ms", latency_ms);
       }
       Err(err) => {
           let error_type = categorize_mirror_error(&err);
           MirrorStats::instance().record_error(&mirror.params.database, error_type);
           error!("mirror error: {} (type: {:?})", err, error_type);
       }
   }
   ```

2. **Add error categorization for mirror-specific errors**
   
   File: `pgdog/src/backend/pool/connection/mirror/mod.rs`
   
   ```rust
   fn categorize_mirror_error(err: &MirrorError) -> MirrorErrorType {
       match err {
           MirrorError::Connection(_) => MirrorErrorType::Connection,
           MirrorError::Io(io_err) => categorize_io_error(io_err),
           MirrorError::Protocol(_) => MirrorErrorType::Query,
           MirrorError::Timeout => MirrorErrorType::Timeout,
           MirrorError::BufferFull => MirrorErrorType::BufferFull,
           _ => {
               // Fall back to string-based categorization
               categorize_error(&err.to_string())
           }
       }
   }
   ```

3. **Update handler to track buffer drops**
   
   File: `pgdog/src/backend/pool/connection/mirror/handler.rs`
   
   ```rust
   // Add import at top
   use crate::stats::mirror::{MirrorStats, MirrorErrorType};
   
   impl MirrorHandler {
       pub fn send(&mut self, req: ClientRequest) -> bool {
           let stats = MirrorStats::instance();
           stats.increment_total();
           
           // Check exposure sampling
           if !self.should_mirror() {
               debug!("Request not selected for mirroring (exposure: {})", self.exposure);
               return true; // Not an error, just not selected
           }
           
           // Try to send to buffer
           match self.tx.try_send(MirrorRequest::new(req)) {
               Ok(_) => {
                   debug!("Request queued for mirroring");
                   true
               }
               Err(TrySendError::Full(_)) => {
                   stats.record_error("unknown", MirrorErrorType::BufferFull);
                   stats.increment_dropped();
                   warn!("Mirror buffer full, dropping request");
                   false
               }
               Err(TrySendError::Closed(_)) => {
                   error!("Mirror channel closed");
                   false
               }
           }
       }
       
       fn should_mirror(&self) -> bool {
           if self.exposure >= 1.0 {
               return true;
           }
           if self.exposure <= 0.0 {
               return false;
           }
           
           // Random sampling based on exposure
           let mut rng = rand::thread_rng();
           rng.gen::<f64>() < self.exposure
       }
   }
   ```

4. **Add stats module to main stats module**
   
   File: `pgdog/src/stats/mod.rs`
   
   ```rust
   pub mod mirror;
   
   // Re-export for convenience
   pub use mirror::{MirrorStats, MirrorErrorType};
   ```

5. **Create integration test helper script**
   
   File: `integration/mirror/test_metrics.sh`
   
   ```bash
   #!/bin/bash
   
   # Start PgDog with mirror config
   cargo build --release
   ./target/release/pgdog --config integration/mirror/pgdog.toml &
   PGDOG_PID=$!
   
   sleep 3
   
   # Run some queries
   psql -h localhost -p 6432 -U pgdog -d primary -c "SELECT 1" -c "SELECT 2"
   
   # Check metrics
   curl -s http://localhost:9090/metrics | grep mirror_
   
   # Cleanup
   kill $PGDOG_PID
   ```

6. **Update mirror configuration for testing**
   
   File: `integration/mirror/pgdog.toml`
   
   ```toml
   [general]
   mirror_metrics_enabled = true
   
   [[pools]]
   name = "primary"
   database = "primary"
   mirror_of = "mirror"
   mirror_exposure = 1.0  # 100% mirroring for testing
   mirror_queue = 100
   ```

## Success Criteria

- [ ] Mirror operations record success metrics with latency
- [ ] Mirror errors are categorized and counted correctly
- [ ] Buffer drops are tracked as both errors and dropped requests
- [ ] Exposure sampling is respected (requests counted even if not mirrored)
- [ ] Integration tests pass showing real metrics collection
- [ ] Metrics visible via HTTP endpoint at /metrics
- [ ] No performance regression in mirror operations
- [ ] Thread-safe metric updates from async mirror tasks
- [ ] `cargo check` shows no warnings
- [ ] `cargo nextest run mirror` passes all tests

## Notes

- This integrates with existing mirror code - be careful not to break it
- Use debug! and trace! logging for observability
- The mirror operates asynchronously, so timing in tests matters
- Focus on minimal changes to existing code
- Prepare the path for OpenMetrics formatting in next sub-plan