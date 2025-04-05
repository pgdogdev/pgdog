# frozen_string_literal: true

require 'active_record'
require 'rspec'
require 'pg'
require 'toxiproxy'

def ensure_done
  conn =  PG.connect(dbname: 'admin', user: 'admin', password: 'pgdog', port: 6432, host: '127.0.0.1')
  pools = conn.exec 'SHOW POOLS'
  pools.each do |pool|
    expect(pool['sv_active']).to eq('0')
    expect(pool['cl_waiting']).to eq('0')
    expect(pool['out_of_sync']).to eq('0')
  end
  clients = conn.exec 'SHOW CLIENTS'
  clients.each do |client|
    expect(client['state']).to eq('idle')
  end
  servers = conn.exec 'SHOW SERVERS'
  servers.each do |server|
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
