# frozen_string_literal: true

require 'active_record'
require 'rspec'
require 'pg'
require 'toxiproxy'
require 'datadog'
require 'securerandom'
require 'concurrent'

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

def connect(db = 'pgdog', user = 'pgdog')
  PG.connect(dbname: db, user: user, password: 'pgdog', port: 6432, host: '127.0.0.1')
end

# "Pin" a backend server by holding it inside an open transaction so the pool
# cannot hand it to any other connection. Relies on pgdog's LIFO idle-connection
# reuse: call this immediately after the query whose backend you want to hold
# (e.g. a PREPARE), before any other query, so the checkout reclaims that exact
# backend. Lets a test verify how that backend's state affects other connections.
def with_pinned_backend
  pin = connect
  pin.exec('BEGIN')
  # CREATE TEMP TABLE is a write, so pgdog routes this open transaction to the
  # primary pool and holds one of its backends. Simple-protocol PREPARE/EXECUTE
  # also route to the primary (pgdog treats them as writes), so the pin sits in
  # the same pool the EXECUTE checks out from - that is what lets it force the
  # EXECUTE onto a different backend.
  pin.exec('CREATE TEMP TABLE pin_block (x int)')
  yield
ensure
  pin.exec('ROLLBACK') rescue nil
  pin.close rescue nil
end

# Runs 3 background connections doing short transactions for the block's
# duration to keep backends busy; stopped and joined when the block ends.
# The churn breaks pgdog's LIFO reuse, so a connection's repeated requests for
# the same statement land on different backends instead of always the same one.
def with_load
  running = Concurrent::AtomicBoolean.new(true)
  threads = 3.times.map do
    Thread.new do
      c = nil
      begin
        c = connect
        while running.true?
          c.exec('BEGIN')
          c.exec('SELECT 1')
          c.exec('COMMIT')
        end
      rescue PG::Error
        nil
      ensure
        c.close rescue nil
      end
    end
  end
  yield
ensure
  running.make_false
  threads.each(&:join)
end

def ensure_done
  deadline = Time.now + 2
  pools = []
  clients = []
  servers = []
  pg_clients = []
  current_client_id = nil

  loop do
    conn = PG.connect(dbname: 'admin', user: 'admin', password: 'pgdog', port: 6432, host: '127.0.0.1')
    begin
      pools = conn.exec 'SHOW POOLS'
      current_client_id = conn.backend_pid
      clients = conn.exec 'SHOW CLIENTS'
      servers = conn.exec 'SHOW SERVERS'
    ensure
      conn.close
    end

    pg_conn = PG.connect(dbname: 'pgdog', user: 'pgdog', password: 'pgdog', port: 5432, host: '127.0.0.1')
    begin
      pg_clients = pg_conn.exec 'SELECT state FROM pg_stat_activity'\
        " WHERE datname IN ('pgdog', 'shard_0', 'shard_1')"\
        " AND backend_type = 'client backend'"\
        " AND query NOT LIKE '%pg_stat_activity%'"
    ensure
      pg_conn.close
    end

    pools_ready = pools.all? do |pool|
      pool['sv_active'] == '0' && pool['cl_waiting'] == '0' && pool['out_of_sync'] == '0'
    end
    clients_ready = clients.all? do |client|
      client['id'].to_i == current_client_id || client['state'] == 'idle'
    end
    servers_ready = servers
      .select { |server| server['application_name'] != 'PgDog Pub/Sub Listener' }
      .all? { |server| server['state'] == 'idle' }
    pg_clients_ready = pg_clients.all? { |client| client['state'] == 'idle' }

    break if pools_ready && clients_ready && servers_ready && pg_clients_ready
    break if Time.now >= deadline

    sleep 0.05
  end

  pools.each do |pool|
    expect(pool['sv_active']).to eq('0')
    expect(pool['cl_waiting']).to eq('0')
    expect(pool['out_of_sync']).to eq('0')
  end

  clients.each do |client|
    next if client['id'].to_i == current_client_id
    expect(client['state']).to eq('idle')
  end

  servers
    .select do |server|
      server['application_name'] != 'PgDog Pub/Sub Listener'
    end
    .each do |server|
      expect(server['state']).to eq('idle')
    end

  pg_clients.each do |client|
    expect(client['state']).to eq('idle')
  end
end
