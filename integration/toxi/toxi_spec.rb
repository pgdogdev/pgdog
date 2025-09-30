# frozen_string_literal: true

require_relative 'rspec_helper'

class Sharded < ActiveRecord::Base
  self.table_name = 'sharded'
  self.primary_key = 'id'
end

def ar_conn(db, prepared)
  ActiveRecord::Base.establish_connection(
    adapter: 'postgresql',
    host: '127.0.0.1',
    port: 6432,
    database: db,
    password: 'pgdog',
    user: 'pgdog',
    prepared_statements: prepared
  )
end

def warm_up
  conn.exec 'SELECT 1'
  admin.exec 'RECONNECT'
  sleep 1
  conn.exec 'SELECT 1'
end

shared_examples 'minimal errors' do |role, toxic|
  it 'executes with reconnecting' do
    Toxiproxy[role].toxic(toxic).apply do
      errors = 0
      25.times do
        c = conn
        c.exec 'SELECT 1::bigint AS one'
        c.close
      rescue StandardError
        errors += 1
      end
      expect(errors).to be < 3
    end
  end

  it 'some connections survive' do
    threads = []
    errors = Concurrent::AtomicFixnum.new(0)
    sem = Concurrent::Semaphore.new(0)
    25.times do
      t = Thread.new do
        c = 1
        sem.acquire
        loop do
          c = conn
          break
        rescue StandardError
          errors.increment
        end
        25.times do
          c.exec 'SELECT 1'
        rescue PG::SystemError
          c = conn # reconnect
          errors.increment
        end
      end
      threads << t
    end
    Toxiproxy[role].toxic(toxic).apply do
      sem.release(25)
      threads.each(&:join)
    end
    expect(errors.value).to be < 25 # 5% error rate (instead of 100%)
  end

  it 'active record works' do
    # Create connection pool.
    ar_conn('failover', true)
    # Connect (the pool is lazy)
    Sharded.where(id: 1).first
    errors = 0
    ok = 0
    # Can't ban primary because it issues SET queries
    # that we currently route to primary.
    Toxiproxy[role].toxic(toxic).apply do
      25.times do
        Sharded.where(id: 1).first
        ok += 1
      rescue StandardError
        errors += 1
      end
    end
    expect(errors).to be <= 1
    expect(25 - ok).to eq(errors)
  end
end

describe 'healthcheck' do
  before do
    admin_conn = admin
    admin_conn.exec "SET read_write_split TO 'exclude_primary'"
    admin_conn.exec 'SET ban_timeout TO 1'
  end

  describe 'will heal itself' do
    def health(role, field = 'healthy')
      admin.exec('SHOW POOLS').select do |pool|
        pool['database'] == 'failover' && pool['role'] == role
      end.map { |pool| pool[field] }
    end

    it 'replica' do
      10.times do
        # Cache connect params.
        conn.exec 'SELECT 1'

        Toxiproxy[:replica].toxic(:reset_peer).apply do
          4.times do
            conn.exec 'SELECT 1'
          rescue PG::Error
          end
          expect(health('replica')).to include('f')
          sleep(0.4)
          expect(health('replica', 'banned')).to include('t')
        end

        4.times do
          conn.exec 'SELECT 1'
        end

        admin.exec 'HEALTHCHECK'
        sleep(0.4)

        expect(health('replica')).to eq(%w[t t t])
        expect(health('replica', 'banned')).to eq(%w[f f f])
      end
    end

    it 'primary' do
      # Cache connect params.
      conn.exec 'DELETE FROM sharded'

      Toxiproxy[:primary].toxic(:reset_peer).apply do
        begin
          conn.exec 'DELETE FROM sharded'
        rescue PG::Error
        end
        expect(health('primary')).to eq(['f'])
      end

      conn.exec 'DELETE FROM sharded'

      expect(health('primary')).to eq(%w[t])
    end
  end

  after do
    admin.exec 'RELOAD'
  end
end

describe 'tcp' do
  around :each do |example|
    Timeout.timeout(30) do
      example.run
    end
  end

  it 'can connect' do
    c = conn
    tup = c.exec 'SELECT 1::bigint AS one'
    expect(tup[0]['one']).to eq('1')
  end

  describe 'broken database' do
    before do
      warm_up
    end

    after do
      admin.exec 'RECONNECT'
    end

    describe 'broken primary' do
      it_behaves_like 'minimal errors', :primary, :reset_peer
    end

    describe 'broken primary with existing conns' do
      it_behaves_like 'minimal errors', :primary, :reset_peer
    end

    describe 'broken replica' do
      it_behaves_like 'minimal errors', :replica, :reset_peer
      it_behaves_like 'minimal errors', :replica2, :reset_peer
      it_behaves_like 'minimal errors', :replica3, :reset_peer
    end

    describe 'timeout primary' do
      describe 'cancels query' do
        it_behaves_like 'minimal errors', :primary, :timeout
      end

      after do
        admin.exec 'RELOAD'
      end
    end

    describe 'both down' do
      it 'unbans all pools' do
        rw_config = admin.exec('SHOW CONFIG').select do |config|
          config['name'] == 'read_write_split'
        end[0]['value']
        expect(rw_config).to eq('include_primary')

        def pool_stat(field, value)
          failover = admin.exec('SHOW POOLS').select do |pool|
            pool['database'] == 'failover'
          end
          entries = failover.select { |item| item[field] == value }
          entries.size
        end

        admin.exec 'SET checkout_timeout TO 100'

        10.times do
          Toxiproxy[:primary].toxic(:reset_peer).apply do
            Toxiproxy[:replica].toxic(:reset_peer).apply do
              Toxiproxy[:replica2].toxic(:reset_peer).apply do
                Toxiproxy[:replica3].toxic(:reset_peer).apply do
                  4.times do
                    conn.exec_params 'SELECT $1::bigint', [1]
                  rescue StandardError
                  end

                  expect(pool_stat('healthy', 'f')).to eq(4)
                end
              end
            end
          end

          4.times do
            conn.exec 'SELECT $1::bigint', [25]
          end
        end
      end
    end

    it 'primary ban is ignored' do
      admin.exec('SHOW POOLS').select do |pool|
        pool['database'] == 'failover'
      end.select { |item| item['banned'] == 'f' }
      Toxiproxy[:primary].toxic(:reset_peer).apply do
        c = conn
        c.exec 'BEGIN'
        c.exec 'CREATE TABLE IF NOT EXISTS test(id BIGINT)'
        c.exec 'ROLLBACK'
      rescue StandardError
      end

      banned = admin.exec('SHOW POOLS').select do |pool|
        pool['database'] == 'failover' && pool['role'] == 'primary'
      end
      expect(banned[0]['healthy']).to eq('f')

      c = conn
      c.exec 'BEGIN'
      c.exec 'CREATE TABLE IF NOT EXISTS test(id BIGINT)'
      c.exec 'SELECT * FROM test'
      c.exec 'ROLLBACK'
    end

    it 'active record works' do
      # Create connection pool.
      ar_conn('failover', true)
      # Connect (the pool is lazy)
      Sharded.where(id: 1).first
      errors = 0
      ok = 0
      # Can't ban primary because it issues SET queries
      # that we currently route to primary.
      Toxiproxy[:primary].toxic(:reset_peer).apply do
        25.times do
          Sharded.where(id: 1).first
          ok += 1
        rescue StandardError
          errors += 1
        end
      end
      expect(errors).to be <= 1
      expect(25 - ok).to eq(errors)
    end
  end
end
