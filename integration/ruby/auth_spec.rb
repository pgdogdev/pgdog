# frozen_string_literal: true

require_relative 'rspec_helper'

describe 'authentication' do
  before(:all) do
    # Reset state so passthrough auth from previous runs is disabled.
    adm = PG.connect('postgres://admin:pgdog@127.0.0.1:6432/admin')
    adm.exec 'RELOAD'
    adm.close
  end

  it 'rejects connections with bad password' do
    expect do
      PG.connect(dbname: 'pgdog', user: 'pgdog', password: 'wrong_password', port: 6432, host: '127.0.0.1')
    end.to raise_error(PG::ConnectionBad, /password for user "pgdog" and database "pgdog" is wrong/)
  end

  it 'rejects connections with bad user' do
    expect do
      PG.connect(dbname: 'pgdog', user: 'nonexistent_user', password: 'pgdog', port: 6432, host: '127.0.0.1')
    end.to raise_error(PG::ConnectionBad, /password for user "nonexistent_user" and database "pgdog" is wrong/)
  end

  shared_examples 'passthrough authentication' do
    it 'authenticates pgdog_pass user via passthrough' do
      adm = admin

      # Reload to reset state, then enable passthrough auth.
      adm.exec 'RELOAD'
      adm.exec "SET passthrough_auth TO 'enabled_plain'"

      # Verify pool starts as offline.
      pools = adm.exec 'SHOW POOLS'
      pass_pool = pools.select { |p| p['user'] == 'pgdog_pass' && p['database'] == 'pgdog' }.first
      expect(pass_pool).not_to be_nil
      expect(pass_pool['online']).to eq('f')

      # Connect with passthrough user and run queries.
      conn = PG.connect(dbname: 'pgdog', user: 'pgdog_pass', password: 'pgdog', port: 6432, host: '127.0.0.1')
      res = conn.exec 'SELECT 1 AS one'
      expect(res[0]['one']).to eq('1')
      res = conn.exec 'SELECT 2 AS two'
      expect(res[0]['two']).to eq('2')
      conn.close

      # Verify pool is now online.
      pools = adm.exec 'SHOW POOLS'
      pass_pool = pools.select { |p| p['user'] == 'pgdog_pass' && p['database'] == 'pgdog' }.first
      expect(pass_pool).not_to be_nil
      expect(pass_pool['online']).to eq('t')

      adm.close
    end
  end

  10.times do |i|
    context "passthrough attempt #{i + 1}" do
      include_examples 'passthrough authentication'
    end
  end
end
