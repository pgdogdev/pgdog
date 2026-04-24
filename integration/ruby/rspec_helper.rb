# frozen_string_literal: true

require 'active_record'
require 'rspec'
require 'pg'
require 'toxiproxy'
require 'datadog'
require 'securerandom'

# Configure Datadog APM so ActiveRecord auto-injects sqlcommenter-style
# trailing comments (traceparent, tracestate, dddbs, dde, ddps, ddpv, ddh,
# controller/action metadata, etc.) on every query. This lets the PgDog
# comment parser be exercised against realistic Rails-with-APM traffic.
Datadog.configure do |c|
  c.service = 'pgdog-integration'
  c.env = 'test'
  c.version = '1.0.0'

  c.diagnostics.startup_logs.enabled = false
  c.logger.level = ::Logger::ERROR

  # Keep spans in memory — no agent is running in CI.
  c.tracing.test_mode.enabled = true

  # Inject full sqlcommenter comments (service info + W3C traceparent) into
  # every ActiveRecord and pg query.
  c.tracing.instrument :active_record, comments_propagation: :full
  c.tracing.instrument :pg, comments_propagation: :full

  # Emit IDs in every propagation style so downstream systems (including
  # the PgDog comment parser) see the full matrix.
  c.tracing.propagation_style = %w[datadog tracecontext b3 b3multi baggage]
end

def admin
  PG.connect('postgres://admin:pgdog@127.0.0.1:6432/admin')
end

def failover
  PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/failover')
end

def admin_stats(database, column = nil)
  conn = admin
  stats = conn.exec 'SHOW STATS'
  conn.close
  stats = stats.select { |item| item['database'] == database }
  return stats.map { |item| item[column].to_i } unless column.nil?

  stats
end

def ensure_done
  conn =  PG.connect(dbname: 'admin', user: 'admin', password: 'pgdog', port: 6432, host: '127.0.0.1')
  pools = conn.exec 'SHOW POOLS'
  pools.each do |pool|
    expect(pool['sv_active']).to eq('0')
    expect(pool['cl_waiting']).to eq('0')
    expect(pool['out_of_sync']).to eq('0')
  end
  current_client_id = conn.backend_pid
  clients = conn.exec 'SHOW CLIENTS'
  clients.each do |client|
    next if client['id'].to_i == current_client_id

    expect(client['state']).to eq('idle')
  end
  servers = conn.exec 'SHOW SERVERS'
  servers
    .select do |server|
      server['application_name'] != 'PgDog Pub/Sub Listener'
    end
    .each do |server|
      expect(server['state']).to eq('idle')
    end

  conn = PG.connect(dbname: 'pgdog', user: 'pgdog', password: 'pgdog', port: 5432, host: '127.0.0.1')
  clients = conn.exec 'SELECT state FROM pg_stat_activity'\
    " WHERE datname IN ('pgdog', 'shard_0', 'shard_1')"\
    " AND backend_type = 'client backend'"\
    " AND query NOT LIKE '%pg_stat_activity%'"
  clients.each do |client|
    expect(client['state']).to eq('idle')
  end
end
