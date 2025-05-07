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
    errors = 0
    sem = Concurrent::Semaphore.new(0)
    (5.0 / 25 * 25.0).ceil
    25.times do
      t = Thread.new do
        c = 1
        sem.acquire
        loop do
          c = conn
          break
        rescue StandardError
          errors += 1
        end
        25.times do
          c.exec 'SELECT 1'
        rescue PG::SystemError
          c = conn # reconnect
          errors += 1
        end
      end
      threads << t
    end
    Toxiproxy[role].toxic(toxic).apply do
      sem.release(25)
      threads.each(&:join)
    end
    expect(errors).to be < 25 # 5% error rate (instead of 100%)
  end

  it 'active record works' do
    # Create connection pool.
    ar_conn('failover', true)
    # Connect (the pool is lazy)
    Sharded.where(id: 1).first
    errors = 0
    # Can't ban primary because it issues SET queries
    # that we currently route to primary.
    Toxiproxy[role].toxic(toxic).apply do
      25.times do
        Sharded.where(id: 1).first
      rescue StandardError
        errors += 1
      end
    end
    expect(errors).to eq(1)
  end
end

describe 'tcp' do
  around :each do |example|
    Timeout.timeout(10) do
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
        25.times do
          Toxiproxy[:primary].toxic(:reset_peer).apply do
            Toxiproxy[:replica].toxic(:reset_peer).apply do
              Toxiproxy[:replica2].toxic(:reset_peer).apply do
                Toxiproxy[:replica3].toxic(:reset_peer).apply do
                  4.times do
                    conn.exec_params 'SELECT $1::bigint', [1]
                  rescue StandardError
                  end
                  banned = admin.exec('SHOW POOLS').select do |pool|
                    pool['database'] == 'failover'
                  end.select { |item| item['banned'] == 't' }
                  expect(banned.size).to eq(4)
                end
              end
            end
          end
          conn.exec 'SELECT $1::bigint', [25]
          banned = admin.exec('SHOW POOLS').select do |pool|
            pool['database'] == 'failover'
          end.select { |item| item['banned'] == 't' }
          expect(banned.size).to eq(0)
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
        c.exec 'CREATE TABLE test(id BIGINT)'
        c.exec 'ROLLBACK'
      rescue StandardError
      end
      banned = admin.exec('SHOW POOLS').select do |pool|
        pool['database'] == 'failover' && pool['role'] == 'primary'
      end
      expect(banned[0]['banned']).to eq('t')

      c = conn
      c.exec 'BEGIN'
      c.exec 'CREATE TABLE test(id BIGINT)'
      c.exec 'SELECT * FROM test'
      c.exec 'ROLLBACK'

      banned = admin.exec('SHOW POOLS').select do |pool|
        pool['database'] == 'failover' && pool['role'] == 'primary'
      end
      expect(banned[0]['banned']).to eq('f')
    end

    it 'active record works' do
      # Create connection pool.
      ar_conn('failover', true)
      # Connect (the pool is lazy)
      Sharded.where(id: 1).first
      errors = 0
      # Can't ban primary because it issues SET queries
      # that we currently route to primary.
      Toxiproxy[:primary].toxic(:reset_peer).apply do
        25.times do
          Sharded.where(id: 1).first
        rescue StandardError
          errors += 1
        end
      end
      expect(errors).to eq(1)
    end
  end
end
