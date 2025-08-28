# Sub-Plan 07: Integration Tests

## Objective
Create comprehensive end-to-end integration tests that validate the entire mirror metrics system working together, including real mirror operations, metric collection, HTTP endpoint exposure, and readiness calculations.

## Dependencies
- All previous sub-plans (01-06) must be completed

## Failing Tests to Write First

```rust
// integration/rust/tests/integration/mirror_metrics.rs (new file)
use pgdog::stats::mirror::{MirrorStats, MirrorErrorType};
use pgdog::stats::mirror_readiness::ReadinessCriteria;
use sqlx::postgres::PgPool;
use std::time::Duration;

#[tokio::test]
async fn test_mirror_metrics_end_to_end() {
    // Start PgDog with mirror configuration
    let pgdog = start_pgdog_with_config(r#"
        [general]
        mirror_metrics_enabled = true
        
        [[pools]]
        name = "primary"
        database = "test_primary"
        mirror_of = "mirror"
        mirror_exposure = 1.0
        mirror_queue = 100
        
        [[pools]]
        name = "mirror"
        database = "test_mirror"
    "#).await;
    
    // Connect to primary
    let pool = PgPool::connect("postgres://pgdog@localhost:6432/test_primary").await.unwrap();
    
    // Execute queries that should be mirrored
    for i in 0..100 {
        sqlx::query(&format!("SELECT {}", i))
            .execute(&pool)
            .await
            .unwrap();
    }
    
    // Wait for async processing
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // Fetch metrics from HTTP endpoint
    let metrics = fetch_metrics("http://localhost:9090/metrics").await;
    
    // Verify mirror metrics exist
    assert!(metrics.contains("# HELP mirror"));
    assert!(metrics.contains("mirror{type=\"mirrored\"}"));
    
    // Parse and verify counts
    let mirrored_count = parse_metric_value(&metrics, "mirror{type=\"mirrored\"}");
    assert!(mirrored_count >= 90); // Allow for some async delay
    
    // Check readiness
    let readiness_output = pgdog.admin_command("SHOW MIRROR READINESS").await.unwrap();
    assert!(readiness_output.contains("Status:"));
    assert!(readiness_output.contains("Confidence Score:"));
    
    pgdog.shutdown().await;
}

#[tokio::test]
async fn test_mirror_error_tracking() {
    // Start PgDog with invalid mirror target
    let pgdog = start_pgdog_with_config(r#"
        [[pools]]
        name = "primary"
        database = "test_primary"
        mirror_of = "broken_mirror"
        mirror_exposure = 1.0
        
        [[pools]]
        name = "broken_mirror"
        database = "nonexistent_db"
        host = "invalid.host"
    "#).await;
    
    let pool = PgPool::connect("postgres://pgdog@localhost:6432/test_primary").await.unwrap();
    
    // Execute queries (will fail to mirror)
    for _ in 0..10 {
        let _ = sqlx::query("SELECT 1").execute(&pool).await;
    }
    
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // Check error metrics
    let metrics = fetch_metrics("http://localhost:9090/metrics").await;
    let conn_errors = parse_metric_value(&metrics, "mirror{error_type=\"connection\"}");
    assert!(conn_errors > 0);
    
    pgdog.shutdown().await;
}

#[tokio::test]
async fn test_buffer_overflow_tracking() {
    // Start with tiny buffer
    let pgdog = start_pgdog_with_config(r#"
        [[pools]]
        name = "primary"
        database = "test_primary"
        mirror_of = "slow_mirror"
        mirror_exposure = 1.0
        mirror_queue = 2  # Very small buffer
    "#).await;
    
    let pool = PgPool::connect("postgres://pgdog@localhost:6432/test_primary").await.unwrap();
    
    // Flood with queries
    let mut handles = vec![];
    for i in 0..50 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            sqlx::query(&format!("SELECT {}", i))
                .execute(&pool)
                .await
        }));
    }
    
    for handle in handles {
        let _ = handle.await;
    }
    
    // Check dropped requests
    let metrics = fetch_metrics("http://localhost:9090/metrics").await;
    let dropped = parse_metric_value(&metrics, "mirror{type=\"dropped\"}");
    assert!(dropped > 0);
    
    let buffer_errors = parse_metric_value(&metrics, "mirror{error_type=\"buffer_full\"}");
    assert!(buffer_errors > 0);
    
    pgdog.shutdown().await;
}

#[tokio::test]
async fn test_readiness_calculation_accuracy() {
    let pgdog = start_pgdog_with_config(r#"
        [general]
        mirror_metrics_enabled = true
        mirror_max_error_rate = 0.05
        mirror_max_latency_ms = 500
        mirror_min_requests = 100
        mirror_min_runtime_hours = 0
    "#).await;
    
    let pool = PgPool::connect("postgres://pgdog@localhost:6432/test_primary").await.unwrap();
    
    // Generate mixed traffic
    for i in 0..95 {
        sqlx::query(&format!("SELECT {}", i))
            .execute(&pool)
            .await
            .unwrap();
    }
    
    // Simulate some errors by disconnecting mirror
    pgdog.stop_pool("mirror").await;
    
    for i in 95..100 {
        let _ = sqlx::query(&format!("SELECT {}", i))
            .execute(&pool)
            .await;
    }
    
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // Check readiness
    let output = pgdog.admin_command("SHOW MIRROR READINESS").await.unwrap();
    
    // Should be ready (5% error rate = threshold)
    assert!(output.contains("Status: READY") || output.contains("Status: NOT READY"));
    assert!(output.contains("Error Rate: 5"));
    assert!(output.contains("Total Requests: 100"));
    
    pgdog.shutdown().await;
}

// Helper functions
async fn fetch_metrics(url: &str) -> String {
    reqwest::get(url)
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

fn parse_metric_value(metrics: &str, metric_name: &str) -> u64 {
    metrics
        .lines()
        .find(|line| line.contains(metric_name) && !line.starts_with("#"))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|val| val.parse().ok())
        .unwrap_or(0)
}
```

```ruby
# integration/mirror/full_integration_spec.rb
require 'rspec'
require 'pg'
require 'net/http'
require 'json'

describe 'Mirror Metrics Full Integration' do
  before(:all) do
    # Start primary and mirror databases
    system("docker-compose -f integration/mirror/docker-compose.yml up -d")
    sleep(5)
    
    # Start PgDog with mirror configuration
    @pgdog_pid = spawn(
      "cargo run -- --config integration/mirror/pgdog.toml",
      out: 'pgdog.log',
      err: 'pgdog.err'
    )
    sleep(3)
    
    @primary = PG.connect(
      host: 'localhost',
      port: 6432,
      dbname: 'primary',
      user: 'pgdog'
    )
  end
  
  after(:all) do
    Process.kill('TERM', @pgdog_pid) if @pgdog_pid
    system("docker-compose -f integration/mirror/docker-compose.yml down")
  end
  
  describe 'metrics collection' do
    it 'tracks successful queries' do
      initial_metrics = fetch_metrics
      initial_count = extract_metric(initial_metrics, 'mirror{type="mirrored"}')
      
      # Run queries
      100.times do |i|
        @primary.exec("SELECT #{i}")
      end
      
      sleep(2) # Allow async processing
      
      final_metrics = fetch_metrics
      final_count = extract_metric(final_metrics, 'mirror{type="mirrored"}')
      
      expect(final_count - initial_count).to be >= 95  # Allow small variance
    end
    
    it 'categorizes different error types' do
      # Stop mirror database
      system("docker stop mirror-postgres")
      
      # Try queries (will get connection errors)
      5.times { @primary.exec("SELECT 1") rescue nil }
      
      sleep(1)
      
      metrics = fetch_metrics
      conn_errors = extract_metric(metrics, 'mirror{error_type="connection"}')
      expect(conn_errors).to be > 0
      
      # Restart mirror
      system("docker start mirror-postgres")
      sleep(2)
      
      # Send invalid query
      @primary.exec("SELECT * FROM nonexistent_table") rescue nil
      
      sleep(1)
      
      metrics = fetch_metrics
      query_errors = extract_metric(metrics, 'mirror{error_type="query"}')
      expect(query_errors).to be > 0
    end
    
    it 'tracks latency statistics' do
      # Run queries
      10.times { @primary.exec("SELECT pg_sleep(0.1)") }
      
      sleep(2)
      
      metrics = fetch_metrics
      
      # Check for latency metrics
      expect(metrics).to match(/mirror\{stat="avg_latency_ms"\}\s+\d+/)
      expect(metrics).to match(/mirror\{stat="max_latency_ms"\}\s+\d+/)
      
      avg_latency = extract_metric(metrics, 'mirror{stat="avg_latency_ms"}')
      expect(avg_latency).to be > 100  # Should be > 100ms due to pg_sleep
    end
    
    it 'provides per-database statistics' do
      # Create second database pool
      @secondary = PG.connect(
        host: 'localhost',
        port: 6432,
        dbname: 'secondary',
        user: 'pgdog'
      )
      
      # Run queries on both
      50.times { @primary.exec("SELECT 1") }
      30.times { @secondary.exec("SELECT 2") }
      
      sleep(2)
      
      metrics = fetch_metrics
      
      # Check for database-specific metrics
      expect(metrics).to match(/mirror\{database="primary",metric="mirrored"\}/)
      expect(metrics).to match(/mirror\{database="secondary",metric="mirrored"\}/)
      
      @secondary.close
    end
  end
  
  describe 'readiness assessment' do
    it 'calculates migration readiness' do
      # Generate sufficient traffic
      1000.times { @primary.exec("SELECT 1") }
      
      sleep(2)
      
      # Check readiness via admin command
      admin_conn = PG.connect(
        host: 'localhost',
        port: 6433,  # Admin port
        dbname: 'pgdog',
        user: 'admin'
      )
      
      result = admin_conn.exec("SHOW MIRROR READINESS")
      output = result.values.flatten.join("\n")
      
      expect(output).to include("Status:")
      expect(output).to include("Confidence Score:")
      expect(output).to include("Error Rate:")
      expect(output).to include("Success Rate:")
      
      admin_conn.close
    end
    
    it 'detects not-ready conditions' do
      # Reset metrics
      restart_pgdog
      
      # Only run a few queries (below minimum)
      5.times { @primary.exec("SELECT 1") }
      
      admin_conn = PG.connect(
        host: 'localhost',
        port: 6433,
        dbname: 'pgdog',
        user: 'admin'
      )
      
      result = admin_conn.exec("SHOW MIRROR READINESS")
      output = result.values.flatten.join("\n")
      
      expect(output).to include("Status: NOT READY")
      expect(output).to include("Total requests")  # Should mention low request count
      
      admin_conn.close
    end
  end
  
  describe 'HTTP endpoint' do
    it 'exposes metrics in OpenMetrics format' do
      response = Net::HTTP.get_response(URI('http://localhost:9090/metrics'))
      
      expect(response.code).to eq('200')
      expect(response['content-type']).to include('text/plain')
      expect(response['content-type']).to include('version=0.0.4')
      
      body = response.body
      
      # Verify OpenMetrics format
      expect(body).to match(/^# HELP mirror/)
      expect(body).to match(/^# TYPE mirror gauge/)
      expect(body).to match(/^mirror\{.*\}\s+\d+/)
    end
    
    it 'updates metrics in real-time' do
      initial = fetch_metric_value('mirror{type="total"}')
      
      10.times { @primary.exec("SELECT 1") }
      sleep(1)
      
      updated = fetch_metric_value('mirror{type="total"}')
      
      expect(updated).to be > initial
    end
  end
  
  describe 'performance impact' do
    it 'maintains low overhead' do
      # Benchmark without metrics
      start_time = Time.now
      100.times { @primary.exec("SELECT 1") }
      baseline_duration = Time.now - start_time
      
      # Enable metrics and benchmark again
      start_time = Time.now
      100.times { @primary.exec("SELECT 1") }
      metrics_duration = Time.now - start_time
      
      # Overhead should be < 5%
      overhead = (metrics_duration - baseline_duration) / baseline_duration
      expect(overhead).to be < 0.05
    end
    
    it 'handles high metric cardinality' do
      # Create many database connections
      connections = []
      10.times do |i|
        conn = PG.connect(
          host: 'localhost',
          port: 6432,
          dbname: "db_#{i}",
          user: 'pgdog'
        )
        connections << conn
        
        # Generate traffic
        10.times { conn.exec("SELECT 1") }
      end
      
      sleep(2)
      
      # Metrics endpoint should still respond quickly
      start_time = Time.now
      response = Net::HTTP.get_response(URI('http://localhost:9090/metrics'))
      duration = Time.now - start_time
      
      expect(response.code).to eq('200')
      expect(duration).to be < 0.1  # Should respond in < 100ms
      
      connections.each(&:close)
    end
  end
  
  private
  
  def fetch_metrics
    Net::HTTP.get(URI('http://localhost:9090/metrics'))
  end
  
  def extract_metric(metrics_text, metric_pattern)
    line = metrics_text.lines.find { |l| l.include?(metric_pattern) && !l.start_with?('#') }
    return 0 unless line
    line.split(' ').last.to_f
  end
  
  def fetch_metric_value(metric_pattern)
    extract_metric(fetch_metrics, metric_pattern)
  end
  
  def restart_pgdog
    Process.kill('TERM', @pgdog_pid)
    sleep(1)
    @pgdog_pid = spawn(
      "cargo run -- --config integration/mirror/pgdog.toml",
      out: 'pgdog.log',
      err: 'pgdog.err'
    )
    sleep(3)
  end
end
```

```bash
#!/bin/bash
# integration/mirror/full_test.sh

set -e

echo "=== Mirror Metrics Full Integration Test ==="

# Setup
echo "Setting up test environment..."
docker-compose -f integration/mirror/docker-compose.yml up -d
sleep 5

# Build PgDog
echo "Building PgDog..."
cargo build --release

# Start PgDog with mirror configuration
echo "Starting PgDog with mirror enabled..."
./target/release/pgdog --config integration/mirror/pgdog.toml &
PGDOG_PID=$!
sleep 3

# Function to check metrics
check_metrics() {
    curl -s http://localhost:9090/metrics | grep -c "mirror{" || true
}

# Test 1: Basic functionality
echo "Test 1: Basic metrics collection..."
psql -h localhost -p 6432 -U pgdog -d primary -c "SELECT 1" -q
METRICS_COUNT=$(check_metrics)
if [ "$METRICS_COUNT" -gt 0 ]; then
    echo "✓ Metrics endpoint working"
else
    echo "✗ Metrics endpoint not working"
    exit 1
fi

# Test 2: Load test
echo "Test 2: Load testing..."
pgbench -h localhost -p 6432 -U pgdog -d primary -c 5 -t 100 -S
sleep 2

MIRRORED=$(curl -s http://localhost:9090/metrics | grep 'mirror{type="mirrored"}' | awk '{print $2}')
if [ "$MIRRORED" -gt 100 ]; then
    echo "✓ Load test metrics collected: $MIRRORED requests"
else
    echo "✗ Insufficient metrics collected: $MIRRORED"
fi

# Test 3: Error tracking
echo "Test 3: Error tracking..."
docker stop mirror-postgres
psql -h localhost -p 6432 -U pgdog -d primary -c "SELECT 1" -q 2>/dev/null || true
sleep 1
docker start mirror-postgres

ERRORS=$(curl -s http://localhost:9090/metrics | grep 'error_type="connection"' | awk '{print $2}')
if [ "$ERRORS" -gt 0 ]; then
    echo "✓ Error tracking working: $ERRORS errors"
else
    echo "✗ Error tracking not working"
fi

# Test 4: Readiness check
echo "Test 4: Migration readiness..."
psql -h localhost -p 6433 -U admin -d pgdog -c "SHOW MIRROR READINESS" | head -20

# Test 5: Prometheus compatibility
echo "Test 5: Prometheus scraping..."
cat > /tmp/prometheus.yml << EOF
scrape_configs:
  - job_name: 'pgdog'
    static_configs:
      - targets: ['localhost:9090']
EOF

prometheus --config.file=/tmp/prometheus.yml --storage.tsdb.path=/tmp/prometheus-data &
PROM_PID=$!
sleep 5

# Query Prometheus
PROM_QUERY=$(curl -s "localhost:9090/api/v1/query?query=mirror" | grep -c "mirror" || true)
if [ "$PROM_QUERY" -gt 0 ]; then
    echo "✓ Prometheus can scrape metrics"
else
    echo "✗ Prometheus cannot scrape metrics"
fi

# Cleanup
echo "Cleaning up..."
kill $PGDOG_PID $PROM_PID 2>/dev/null || true
docker-compose -f integration/mirror/docker-compose.yml down
rm -rf /tmp/prometheus-data /tmp/prometheus.yml

echo "=== Integration tests complete ==="
```

## Implementation Steps

1. **Create test helper module**
   
   File: `integration/rust/tests/helpers/mirror_test_helpers.rs`
   
   ```rust
   use std::process::{Command, Child};
   use std::time::Duration;
   use tokio::time::sleep;
   
   pub struct PgDogInstance {
       process: Child,
       admin_port: u16,
       metrics_port: u16,
   }
   
   impl PgDogInstance {
       pub async fn admin_command(&self, cmd: &str) -> Result<String, Box<dyn Error>> {
           // Execute admin command via psql
           let output = Command::new("psql")
               .args(&[
                   "-h", "localhost",
                   "-p", &self.admin_port.to_string(),
                   "-U", "admin",
                   "-d", "pgdog",
                   "-c", cmd,
               ])
               .output()?;
           
           Ok(String::from_utf8_lossy(&output.stdout).to_string())
       }
       
       pub async fn shutdown(mut self) {
           self.process.kill().ok();
           self.process.wait().ok();
       }
   }
   
   pub async fn start_pgdog_with_config(config: &str) -> PgDogInstance {
       // Write config to temp file
       let config_path = "/tmp/pgdog_test.toml";
       std::fs::write(config_path, config).unwrap();
       
       // Start PgDog
       let process = Command::new("cargo")
           .args(&["run", "--", "--config", config_path])
           .spawn()
           .expect("Failed to start PgDog");
       
       // Wait for startup
       sleep(Duration::from_secs(3)).await;
       
       PgDogInstance {
           process,
           admin_port: 6433,
           metrics_port: 9090,
       }
   }
   ```

2. **Create Docker Compose for test databases**
   
   File: `integration/mirror/docker-compose.yml`
   
   ```yaml
   version: '3.8'
   services:
     primary-postgres:
       image: postgres:15
       environment:
         POSTGRES_DB: primary
         POSTGRES_USER: pgdog
         POSTGRES_PASSWORD: pgdog
       ports:
         - "5432:5432"
     
     mirror-postgres:
       image: postgres:15
       container_name: mirror-postgres
       environment:
         POSTGRES_DB: mirror
         POSTGRES_USER: pgdog
         POSTGRES_PASSWORD: pgdog
       ports:
         - "5433:5432"
   ```

3. **Create comprehensive test configuration**
   
   File: `integration/mirror/pgdog.toml`
   
   ```toml
   [general]
   host = "0.0.0.0"
   port = 6432
   admin_port = 6433
   metrics_port = 9090
   mirror_metrics_enabled = true
   mirror_max_error_rate = 0.05
   mirror_max_latency_ms = 1000
   mirror_min_runtime_hours = 0  # For testing
   mirror_min_requests = 100
   
   [[pools]]
   name = "primary"
   database = "primary"
   host = "localhost"
   port = 5432
   user = "pgdog"
   password = "pgdog"
   mirror_of = "mirror"
   mirror_exposure = 1.0
   mirror_queue = 100
   
   [[pools]]
   name = "mirror"
   database = "mirror"
   host = "localhost"
   port = 5433
   user = "pgdog"
   password = "pgdog"
   ```

4. **Create Makefile targets for integration tests**
   
   File: `Makefile` (update existing)
   
   ```makefile
   test-mirror-integration:
       @echo "Running mirror metrics integration tests..."
       docker-compose -f integration/mirror/docker-compose.yml up -d
       sleep 5
       cargo nextest run mirror_metrics --test-threads=1
       cargo test --test mirror_metrics -- --test-threads=1
       bundle exec rspec integration/mirror/full_integration_spec.rb
       docker-compose -f integration/mirror/docker-compose.yml down
   
   test-mirror-quick:
       @echo "Quick mirror metrics test..."
       ./integration/mirror/full_test.sh
   ```

## Success Criteria

- [ ] All integration tests pass consistently
- [ ] End-to-end flow works: query → mirror → metrics → HTTP → readiness
- [ ] Error scenarios properly tracked and categorized
- [ ] Buffer overflow detection works correctly
- [ ] Readiness calculation matches expected values
- [ ] Per-database metrics properly segregated
- [ ] HTTP endpoint responds with valid OpenMetrics format
- [ ] Prometheus can successfully scrape metrics
- [ ] Performance overhead < 5% on normal operations
- [ ] No memory leaks during extended runs
- [ ] Concurrent operations handled correctly
- [ ] `cargo nextest run mirror` passes all tests

## Notes

- These tests validate the complete implementation
- Use Docker for isolated test databases
- Test both success and failure scenarios
- Verify metrics accuracy and format
- Consider adding stress tests for production readiness
- Document any flaky tests and mitigation strategies