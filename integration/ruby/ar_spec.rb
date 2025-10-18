# frozen_string_literal: true

require_relative 'rspec_helper'
require 'pp'
require 'timeout'

class Sharded < ActiveRecord::Base
  self.table_name = 'sharded'
  self.primary_key = 'id'
end

def conn(db, prepared)
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

describe 'active record' do
  after do
    ensure_done
  end
  describe 'normal' do
    before do
      conn('pgdog', false)
      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGINT, value TEXT)'
    end

    it 'can connect' do
      res = ActiveRecord::Base.connection.execute 'SELECT 1 AS one'
      expect(res.num_tuples).to eq(1)
      expect(res[0]['one']).to eq(1)
    end

    it 'can execute normal statements' do
      res = Sharded.create id: 1, value: 'test'
      expect(res.id).to eq(1)
      expect(res.value).to eq('test')
      250.times do
        expect(Sharded.find(1).id).to eq(1)
      end
    end
  end

  describe 'sharded' do
    before do
      conn('pgdog_sharded', false)

      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
    end

    it 'can connect' do
      250.times do
        res = ActiveRecord::Base.connection.execute 'SELECT 1 AS one'
        expect(res.num_tuples).to eq(1)
        expect(res[0]['one']).to eq(1)
      end
    end

    it 'can execute normal statements' do
      250.times do |id|
        res = Sharded.create id: id, value: "value_#{id}"
        expect(res.id).to eq(id)
        expect(res.value).to eq("value_#{id}")
        expect(Sharded.find(id).value).to eq("value_#{id}")
      end
    end

    it 'assigns to shards using round robin' do
      250.times do |i|
        res = Sharded.new
        res.value = 'test'
        created = res.save
        expect(created).to be_truthy
        expect(res.id).to eq(i / 2 + 1)
      end
    end
  end

  describe 'active record prepared' do
    describe 'normal' do
      before do
        conn('pgdog', true)
        ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
        ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
      end

      it 'can create and read record' do
        15.times do |j|
          res = Sharded.create value: 'test'
          expect(res.id).to eq(j + 1)
          250.times do |_i|
            Sharded.find(j + 1)
          end
        end
      end
    end

    describe 'sharded' do
      before do
        conn('pgdog_sharded', true)
        ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
        ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
        # Automatic primary key assignment.
        ActiveRecord::Base.connection.execute "/* pgdog_shard: 0 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 0)"
        ActiveRecord::Base.connection.execute "/* pgdog_shard: 1 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 1)"
        ActiveRecord::Base.connection.execute '/* pgdog_shard: 0 */ SELECT pgdog.install_shard_id(0)'
        ActiveRecord::Base.connection.execute '/* pgdog_shard: 1 */ SELECT pgdog.install_shard_id(1)'
      end

      it 'can create and read record' do
        30.times do |j|
          res = Sharded.create value: "test_#{j}"
          res = Sharded.find(res.id)
          expect(res.value).to eq("test_#{j}")
          count = Sharded.where(id: res.id).count
          expect(count).to eq(1)
        end
      end

      it 'can do transaction' do
        30.times do |i|
          Sharded.transaction do
            # The first query decides which shard this will go to.
            # If it's an insert without a sharding key, pgdog will
            # use round robin to split data evenly.
            res = Sharded.create value: "val_#{i}"
            res = Sharded.find(res.id)
            expect(res.value).to eq("val_#{i}")
          end
        end
        count = Sharded.count
        expect(count).to eq(30)
      end

      it 'can use set' do
        30.times do |i|
          Sharded.create value: "comment_#{i}"
        end
        count = Sharded.count
        expect(count).to eq(30)
        [0, 1].each do |shard|
          Sharded.transaction do
            Sharded.connection.execute "SET pgdog.shard TO #{shard}"
            count = Sharded.count
            expect(count).to be < 30
            shard_id = Sharded
                       .select('pgdog.shard_id() AS shard_id')
                       .first
            expect(shard_id.shard_id).to eq(shard)
          end
        end
      end

      it 'can handle DISCARD ALL with prepared statements' do
        # Create some records to work with
        5.times do |i|
          Sharded.create value: "test_#{i}"
        end

        5.times do |i|
          record = Sharded.where(value: "test_#{i}").first
          expect(record.value).to eq("test_#{i}")
        end

        ActiveRecord::Base.connection.execute 'DISCARD ALL'

        5.times do |i|
          record = Sharded.where(value: "test_#{i}").first
          expect(record.value).to eq("test_#{i}")
        end

        # Verify prepared stataments exist
        new_record = Sharded.create value: 'after_discard'
        expect(new_record.value).to eq('after_discard')
        record = Sharded.where(value: 'after_discard').first
        expect(record.value).to eq('after_discard')

        count = Sharded.count
        expect(count).to eq(6)
      end
    end
  end

  describe 'chaos testing with interrupted queries' do
    before do
      conn('failover', false)
      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
    end

    it 'handles interrupted queries and continues operating normally' do
      interrupted_count = 0
      successful_count = 0
      mutex = Mutex.new

      # Apply latency toxic to slow down query transmission,
      # making it easier to interrupt queries mid-flight
      Toxiproxy[:primary].toxic(:latency, latency: 100, jitter: 50).apply do
        # Phase 1: Chaos - interrupt queries randomly with thread kills
        chaos_threads = []
        killer_threads = []

        # Start 10 query threads
        10.times do |thread_id|
          t = Thread.new do
            100.times do |i|
              begin
                case rand(3)
                when 0
                  # SELECT query
                  Sharded.where('id > ?', 0).limit(10).to_a
                when 1
                  # INSERT query
                  Sharded.create value: "thread_#{thread_id}_iter_#{i}"
                when 2
                  # Transaction with multiple operations
                  Sharded.transaction do
                    rec = Sharded.create value: "tx_#{thread_id}_#{i}"
                    Sharded.where(id: rec.id).first if rec.id
                  end
                end
                mutex.synchronize { successful_count += 1 }
              rescue StandardError => e
                # Killed mid-query or other error
                mutex.synchronize { interrupted_count += 1 }
              end
            end
          end
          chaos_threads << t
        end

        # Start killer thread that randomly kills query threads
        killer = Thread.new do
          50.times do
            sleep(rand(0.01..0.05))
            alive_threads = chaos_threads.select(&:alive?)
            if alive_threads.any?
              victim = alive_threads.sample
              victim.kill
              mutex.synchronize { interrupted_count += 1 }
            end
          end
        end
        killer_threads << killer

        # Wait for killer to finish
        killer_threads.each(&:join)

        # Wait for remaining threads (with timeout)
        chaos_threads.each { |t| t.join(0.1) }

        puts "Chaos phase complete: #{successful_count} successful, #{interrupted_count} interrupted"
        expect(interrupted_count).to be > 0
      end # End toxiproxy latency

      # Give PgDog time to clean up broken connections
      sleep(0.5)

      # Disconnect all connections to clear bad state
      ActiveRecord::Base.connection_pool.disconnect!

      # Wait a bit more for cleanup
      sleep(0.5)

      # Phase 2: Verify database continues to operate normally
      verification_errors = []
      errors_mutex = Mutex.new

      verification_threads = 10.times.map do |thread_id|
        Thread.new do
          20.times do |i|
            begin
              # Simple queries that don't depend on finding specific records
              # INSERT
              rec = Sharded.create value: "verify_#{thread_id}_#{i}"
              expect(rec.id).to be > 0

              # SELECT with basic query
              results = Sharded.where('value LIKE ?', 'verify_%').limit(5).to_a
              expect(results).to be_a(Array)

              # COUNT query
              count = Sharded.where('id > ?', 0).count
              expect(count).to be >= 0
            rescue PG::Error => e
              # PG errors should fail the test
              raise
            rescue StandardError => e
              errors_mutex.synchronize { verification_errors << e }
            end
          end
        end
      end

      verification_threads.each(&:join)

      # Verify no errors occurred during verification
      expect(verification_errors).to be_empty, "Verification errors: #{verification_errors.map(&:message).join(', ')}"

      # Verify we can still execute basic queries
      ActiveRecord::Base.connection.execute('SELECT 1')

      # Verify count works
      count = Sharded.count
      expect(count).to be >= 0
    end
  end
end
