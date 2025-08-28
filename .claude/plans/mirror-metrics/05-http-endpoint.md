# Sub-Plan 05: HTTP Endpoint Integration

## Objective
Integrate MirrorStats with PgDog's existing HTTP metrics endpoint at port 9090, ensuring mirror metrics appear alongside existing pool and client metrics.

## Dependencies
- Sub-plan 01: Core Stats Structure (completed)
- Sub-plan 02: Error Categorization (completed)
- Sub-plan 03: Mirror Integration (completed)
- Sub-plan 04: OpenMetrics Implementation (completed)

## Failing Tests to Write First

```rust
// pgdog/src/stats/http_server.rs (add test module)
#[cfg(test)]
mod http_mirror_tests {
    use super::*;
    use hyper::StatusCode;
    use crate::stats::mirror::MirrorStats;
    
    #[tokio::test]
    async fn test_metrics_endpoint_includes_mirror_stats() {
        // Set up some mirror data
        let mirror_stats = MirrorStats::instance();
        mirror_stats.record_success("test_db", 100);
        mirror_stats.record_error("test_db", MirrorErrorType::Connection);
        
        // Create request
        let req = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(hyper::body::Incoming::default())
            .unwrap();
        
        // Call metrics handler
        let response = metrics(req).await.unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
        
        // Get body
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body_bytes.to_vec()).unwrap();
        
        // Check for mirror metrics
        assert!(body.contains("# HELP mirror"));
        assert!(body.contains("# TYPE mirror gauge"));
        assert!(body.contains("mirror{type=\"total\"}"));
        assert!(body.contains("mirror{type=\"mirrored\"}"));
        assert!(body.contains("mirror{error_type=\"connection\"}"));
    }
    
    #[tokio::test]
    async fn test_metrics_endpoint_format() {
        let mirror_stats = MirrorStats::instance();
        
        // Add various metrics
        for i in 0..5 {
            mirror_stats.record_success("prod_db", 50 + i * 10);
        }
        mirror_stats.record_error("prod_db", MirrorErrorType::Timeout);
        
        let req = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(hyper::body::Incoming::default())
            .unwrap();
        
        let response = metrics(req).await.unwrap();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body_bytes.to_vec()).unwrap();
        
        // Verify proper line formatting
        let lines: Vec<&str> = body.lines().collect();
        
        // Should have mirror metrics
        let mirror_lines: Vec<_> = lines.iter()
            .filter(|l| l.contains("mirror"))
            .collect();
        assert!(!mirror_lines.is_empty());
        
        // Check metric values are present
        let has_value = mirror_lines.iter()
            .any(|l| l.contains("} ") && !l.starts_with("#"));
        assert!(has_value);
    }
    
    #[tokio::test]
    async fn test_metrics_endpoint_content_type() {
        let req = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(hyper::body::Incoming::default())
            .unwrap();
        
        let response = metrics(req).await.unwrap();
        
        // Check OpenMetrics content type
        let content_type = response.headers()
            .get(hyper::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        
        assert!(content_type.contains("text/plain"));
        assert!(content_type.contains("version=0.0.4"));
        assert!(content_type.contains("charset=utf-8"));
    }
}

// Integration test: integration/mirror/http_endpoint_spec.rb
require 'rspec'
require 'net/http'
require 'json'

describe 'Mirror Metrics HTTP Endpoint' do
  before(:all) do
    # Start PgDog with mirror enabled
    @pgdog_pid = spawn(
      "cargo run -- --config integration/mirror/pgdog.toml",
      out: '/dev/null',
      err: 'pgdog.log'
    )
    sleep(3)
    
    @primary = PG.connect(host: 'localhost', port: 6432, dbname: 'primary')
  end
  
  after(:all) do
    Process.kill('TERM', @pgdog_pid) if @pgdog_pid
  end
  
  it 'exposes mirror metrics at /metrics endpoint' do
    # Generate some traffic
    10.times { @primary.exec("SELECT 1") }
    
    sleep(1)
    
    # Fetch metrics
    uri = URI('http://localhost:9090/metrics')
    response = Net::HTTP.get_response(uri)
    
    expect(response.code).to eq('200')
    expect(response['content-type']).to include('text/plain')
    
    body = response.body
    expect(body).to include('# HELP mirror')
    expect(body).to include('mirror{type="mirrored"}')
  end
  
  it 'updates metrics in real-time' do
    # Get initial metrics
    initial = fetch_metric_value('mirror{type="mirrored"}')
    
    # Generate traffic
    5.times { @primary.exec("SELECT NOW()") }
    sleep(1)
    
    # Get updated metrics
    updated = fetch_metric_value('mirror{type="mirrored"}')
    
    expect(updated).to be > initial
  end
  
  it 'includes all metric types' do
    metrics = fetch_all_metrics
    
    # Check for various mirror metric types
    expect(metrics).to match(/mirror\{type="total"\}\s+\d+/)
    expect(metrics).to match(/mirror\{type="mirrored"\}\s+\d+/)
    expect(metrics).to match(/mirror\{type="dropped"\}\s+\d+/)
    expect(metrics).to match(/mirror\{error_type="connection"\}\s+\d+/)
    expect(metrics).to match(/mirror\{error_type="query"\}\s+\d+/)
    expect(metrics).to match(/mirror\{error_type="timeout"\}\s+\d+/)
    expect(metrics).to match(/mirror\{health="consecutive_errors"\}\s+\d+/)
  end
  
  it 'integrates with existing metrics' do
    metrics = fetch_all_metrics
    
    # Should have both mirror and existing metrics
    expect(metrics).to include('# HELP mirror')
    expect(metrics).to include('# HELP pools')
    expect(metrics).to include('# HELP clients')
  end
  
  private
  
  def fetch_all_metrics
    uri = URI('http://localhost:9090/metrics')
    Net::HTTP.get(uri)
  end
  
  def fetch_metric_value(metric_name)
    metrics = fetch_all_metrics
    line = metrics.lines.find { |l| l.include?(metric_name) && !l.start_with?('#') }
    return 0 unless line
    
    # Extract value (format: "metric{labels} value")
    line.split(' ').last.to_f
  end
end
```

## Implementation Steps

1. **Update HTTP metrics handler**
   
   File: `pgdog/src/stats/http_server.rs`
   
   ```rust
   // Add import at top
   use crate::stats::mirror::MirrorStats;
   use crate::stats::open_metric::Metric;
   
   // Update metrics handler (around line 15-35)
   async fn metrics(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
       // Load existing metrics
       let clients = Clients::load();
       let pools = Pools::load();
       
       // Load mirror metrics
       let mirror_stats = MirrorStats::instance();
       let mirror = Metric::new(mirror_stats.as_ref().clone());
       
       // Load query cache metrics
       let query_cache: Vec<_> = QueryCache::load()
           .metrics()
           .into_iter()
           .map(|m| m.to_string())
           .collect();
       let query_cache = query_cache.join("\n");
       
       // Combine all metrics
       let metrics_data = format!(
           "{}\n{}\n{}\n{}", 
           clients, 
           pools, 
           mirror,    // Add mirror metrics here
           query_cache
       );
       
       // Build response with proper content type
       let response = Response::builder()
           .header(
               hyper::header::CONTENT_TYPE,
               "text/plain; version=0.0.4; charset=utf-8",
           )
           .body(Full::new(Bytes::from(metrics_data)))
           .unwrap_or_else(|_| {
               Response::builder()
                   .status(StatusCode::INTERNAL_SERVER_ERROR)
                   .body(Full::new(Bytes::from("Metrics unavailable")))
                   .unwrap()
           });
       
       Ok(response)
   }
   ```

2. **Ensure metrics server starts with mirror support**
   
   File: `pgdog/src/stats/http_server.rs`
   
   ```rust
   pub async fn start(port: u16) -> Result<(), Box<dyn std::error::Error>> {
       let addr = SocketAddr::from(([0, 0, 0, 0], port));
       
       info!("Starting metrics server on port {}", port);
       
       // Ensure mirror stats are initialized
       let _ = MirrorStats::instance();
       
       let service = make_service_fn(|_conn| async {
           Ok::<_, Infallible>(service_fn(handle_request))
       });
       
       let server = Server::bind(&addr).serve(service);
       
       info!("Metrics server listening on http://{}", addr);
       
       if let Err(e) = server.await {
           error!("Metrics server error: {}", e);
       }
       
       Ok(())
   }
   ```

3. **Add configuration check for mirror metrics**
   
   File: `pgdog/src/stats/http_server.rs`
   
   ```rust
   use crate::config::config;
   
   async fn metrics(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
       let clients = Clients::load();
       let pools = Pools::load();
       
       // Only include mirror metrics if enabled
       let mirror_metrics = if config().config.general.mirror_metrics_enabled {
           let mirror_stats = MirrorStats::instance();
           let mirror = Metric::new(mirror_stats.as_ref().clone());
           format!("\n{}", mirror)
       } else {
           String::new()
       };
       
       let query_cache: Vec<_> = QueryCache::load()
           .metrics()
           .into_iter()
           .map(|m| m.to_string())
           .collect();
       let query_cache = query_cache.join("\n");
       
       let metrics_data = format!(
           "{}\n{}{}\n{}", 
           clients, 
           pools, 
           mirror_metrics,
           query_cache
       );
       
       // ... rest of response building
   }
   ```

4. **Create curl test script**
   
   File: `integration/mirror/curl_metrics_test.sh`
   
   ```bash
   #!/bin/bash
   
   echo "Testing mirror metrics via HTTP endpoint..."
   
   # Start PgDog
   cargo build --release
   ./target/release/pgdog --config integration/mirror/pgdog.toml &
   PGDOG_PID=$!
   
   sleep 3
   
   # Generate traffic
   psql -h localhost -p 6432 -U pgdog -d primary << EOF
   SELECT 1;
   SELECT 2;
   SELECT 3;
   EOF
   
   sleep 1
   
   # Test metrics endpoint
   echo "=== Fetching metrics ==="
   curl -s http://localhost:9090/metrics > metrics.txt
   
   # Verify mirror metrics present
   echo "=== Checking for mirror metrics ==="
   grep "^# HELP mirror" metrics.txt || echo "ERROR: No mirror help text"
   grep "^# TYPE mirror" metrics.txt || echo "ERROR: No mirror type"
   grep "^mirror{" metrics.txt || echo "ERROR: No mirror measurements"
   
   # Count mirror metric lines
   MIRROR_COUNT=$(grep "^mirror{" metrics.txt | wc -l)
   echo "Found $MIRROR_COUNT mirror metric lines"
   
   # Check values are non-zero
   grep "^mirror{type=\"total\"}" metrics.txt
   grep "^mirror{type=\"mirrored\"}" metrics.txt
   
   # Cleanup
   kill $PGDOG_PID
   rm metrics.txt
   
   echo "HTTP endpoint test completed"
   ```

5. **Add Prometheus scrape test**
   
   File: `integration/mirror/prometheus_scrape.sh`
   
   ```bash
   #!/bin/bash
   
   # Start PgDog
   ./target/release/pgdog --config integration/mirror/pgdog.toml &
   PGDOG_PID=$!
   
   # Start Prometheus with test config
   prometheus --config.file=integration/mirror/prometheus_test.yml &
   PROM_PID=$!
   
   sleep 5
   
   # Query Prometheus for mirror metrics
   curl -s "http://localhost:9091/api/v1/query?query=mirror" | jq .
   
   # Cleanup
   kill $PGDOG_PID $PROM_PID
   ```

## Success Criteria

- [ ] Mirror metrics appear at http://localhost:9090/metrics
- [ ] Metrics are in valid OpenMetrics format
- [ ] Content-Type header is correct for OpenMetrics
- [ ] Mirror metrics appear alongside existing metrics (clients, pools, etc.)
- [ ] Metrics update in real-time as mirror operations occur
- [ ] Configuration flag `mirror_metrics_enabled` is respected
- [ ] No performance impact on metrics endpoint (< 10ms response time)
- [ ] Prometheus can successfully scrape the endpoint
- [ ] All metric labels and values are properly formatted
- [ ] `cargo check` shows no warnings

## Notes

- This integrates with the existing HTTP server - be careful not to break it
- The metrics endpoint is critical for monitoring - ensure high availability
- Keep the format consistent with existing metrics
- Next sub-plan will add migration readiness calculations
- Consider adding metric caching if performance becomes an issue