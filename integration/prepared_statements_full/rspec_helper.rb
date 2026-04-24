# frozen_string_literal: true

require 'active_record'
require 'rspec'
require 'pg'
require 'toxiproxy'

def admin
  PG.connect('postgres://admin:pgdog@127.0.0.1:6432/admin')
end

def admin_exec(sql)
  conn = admin
  conn.exec sql
ensure
  conn&.close
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


def connect_pgdog(user: 'pgdog')
  PG.connect(dbname: 'pgdog', user:, password: 'pgdog', port: 6432, host: '127.0.0.1')
end