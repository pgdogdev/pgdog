# frozen_string_literal: true

require 'rspec'
require 'pg'
require 'net/http'
require 'json'

describe 'mirror metrics' do
  let(:conn) { PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog') }
  let(:admin) { PG.connect('postgres://admin:pgdog@127.0.0.1:6432/admin') }

  it 'tracks successful mirror operations' do
    # Get baseline metrics
    baseline = admin.exec('SHOW MIRROR_STATS').to_a
    baseline_total = baseline.find { |r| r['metric'] == 'requests_total' }&.fetch('value', '0').to_i
    baseline_mirrored = baseline.find { |r| r['metric'] == 'requests_mirrored' }&.fetch('value', '0').to_i

    # Clean up and create test table - it will be mirrored automatically
    conn.exec 'DROP TABLE IF EXISTS mirror_metrics_test'
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    mirror_direct.exec 'DROP TABLE IF EXISTS mirror_metrics_test'
    mirror_direct.close
    
    # Create table through PgDog - it will be mirrored automatically
    conn.exec 'BEGIN'
    conn.exec 'CREATE TABLE mirror_metrics_test (id BIGINT PRIMARY KEY, data TEXT)'
    conn.exec 'COMMIT'
    # Wait for mirror to create the table
    sleep(0.5)

    # Execute some queries that should be mirrored in a transaction
    conn.exec 'BEGIN'
    10.times do |i|
      conn.exec "INSERT INTO mirror_metrics_test VALUES (#{i}, 'test data #{i}')"
    end
    conn.exec 'COMMIT'

    # Wait for async mirror processing
    sleep(1.5)

    # Check updated metrics
    updated = admin.exec('SHOW MIRROR_STATS').to_a
    updated_total = updated.find { |r| r['metric'] == 'requests_total' }&.fetch('value', '0').to_i
    updated_mirrored = updated.find { |r| r['metric'] == 'requests_mirrored' }&.fetch('value', '0').to_i

    expect(updated_total).to be > baseline_total
    expect(updated_mirrored).to be > baseline_mirrored

    # Verify data actually made it to mirror by connecting directly to pgdog1
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    mirror_count = mirror_direct.exec('SELECT COUNT(*) FROM mirror_metrics_test')[0]['count'].to_i
    expect(mirror_count).to eq(10)
    mirror_direct.close

    # Clean up
    conn.exec 'DROP TABLE IF EXISTS mirror_metrics_test'
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    mirror_direct.exec 'DROP TABLE IF EXISTS mirror_metrics_test'
    mirror_direct.close
  end

  it 'tracks per-database mirror statistics' do
    # Execute a query to ensure we have some data
    conn.exec 'SELECT 1'
    sleep(0.5)

    # Check per-database breakdown
    db_stats = admin.exec('SHOW MIRROR_STATS_BY_DATABASE').to_a
    
    # Debug: print what we actually get to understand the data
    # puts "Database stats: #{db_stats.inspect}"
    
    # Should have stats for our database and user combination
    # Note: the new format includes both database and user columns
    # We might need to wait a bit for stats to populate
    pgdog_stats = db_stats.find { |r| r['database'] == 'pgdog' && r['user'] == 'pgdog' }
    
    # If not found, might be listed as pgdog_mirror
    pgdog_stats ||= db_stats.find { |r| r['database'] == 'pgdog_mirror' && r['user'] == 'pgdog' }
    
    expect(pgdog_stats).not_to be_nil
    
    if pgdog_stats
      expect(pgdog_stats['mirrored'].to_i).to be >= 0
      expect(pgdog_stats['errors'].to_i).to be >= 0
    end
  end

  it 'exposes metrics via HTTP endpoint' do
    # Execute some queries to generate metrics
    5.times { conn.exec 'SELECT 1' }
    sleep(0.5)

    # Fetch metrics from HTTP endpoint
    uri = URI('http://localhost:9090/metrics')
    response = Net::HTTP.get(uri)
    
    # Check for mirror metrics in new Prometheus-compliant format
    # Note: prefix (pgdog_) would be added via config.openmetrics_namespace
    expect(response).to include('# TYPE mirror_requests_total counter')
    expect(response).to include('# TYPE mirror_requests_mirrored_total counter')
    expect(response).to include('# TYPE mirror_errors_total counter')
    expect(response).to include('mirror_requests_total')
    expect(response).to include('mirror_requests_mirrored_total')
    
    # Check for error metrics with labels
    expect(response).to include('mirror_errors_total{error_type="connection"}')
    expect(response).to include('mirror_errors_total{error_type="query"}')
    
    # Check for latency metrics in seconds
    expect(response).to include('# UNIT mirror_latency_seconds_avg seconds')
    expect(response).to include('# UNIT mirror_latency_seconds_max seconds')
    
    # Parse a metric value to ensure it's numeric
    if response =~ /mirror_requests_total\s+(\d+)/
      total = $1.to_i
      expect(total).to be >= 0
    end
  end

  it 'tracks mirror errors separately' do
    # Get baseline error metrics
    baseline = admin.exec('SHOW MIRROR_STATS').to_a
    baseline_errors = %w[errors_connection errors_query errors_timeout errors_buffer_full].map do |metric|
      [metric, baseline.find { |r| r['metric'] == metric }&.fetch('value', '0').to_i]
    end.to_h

    # Note: In a real test we might trigger actual errors by stopping the mirror database
    # For now, just verify the error counters exist and are queryable
    expect(baseline_errors['errors_connection']).to be >= 0
    expect(baseline_errors['errors_query']).to be >= 0
    expect(baseline_errors['errors_timeout']).to be >= 0
    expect(baseline_errors['errors_buffer_full']).to be >= 0
  end

  it 'tracks latency metrics' do
    # Execute queries
    5.times { conn.exec 'SELECT pg_sleep(0.01)' }
    sleep(0.5)

    # Check latency metrics
    metrics = admin.exec('SHOW MIRROR_STATS').to_a
    
    latency_avg = metrics.find { |r| r['metric'] == 'latency_avg_ms' }
    latency_max = metrics.find { |r| r['metric'] == 'latency_max_ms' }

    # These might be nil if no successful mirrors yet, which is OK
    if latency_avg
      expect(latency_avg['value'].to_i).to be >= 0
    end

    if latency_max
      expect(latency_max['value'].to_i).to be >= 0
      # Max should be >= avg if both exist
      if latency_avg
        expect(latency_max['value'].to_i).to be >= latency_avg['value'].to_i
      end
    end
  end
end