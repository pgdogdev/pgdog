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

    it 'handles idle transaction timeout correctly' do
      ActiveRecord::Base.transaction do
        ActiveRecord::Base.connection.execute "SET idle_in_transaction_session_timeout TO '250'"
        ActiveRecord::Base.connection.execute 'SELECT 1'
        sleep(0.3)
        expect do
          ActiveRecord::Base.connection.execute 'SELECT 2'
        end.to raise_error(/terminating connection due to idle-in-transaction timeout/)

        # Catching errors inside a transaction block will cause the rest of it to fail
        # The connection has been closed.
        # PG::ConnectionBad
        expect do
          ActiveRecord::Base.connection.execute 'SELECT 3'
        end.to raise_error(/can't get socket descriptor/)
      end
      ActiveRecord::Base.connection.execute 'SELECT 1'
    end

    it 'handles statement timeout correctly' do
      ActiveRecord::Base.transaction do
        ActiveRecord::Base.connection.execute "SET statement_timeout TO '100'"
        ActiveRecord::Base.connection.execute 'SELECT 1'
        expect do
          ActiveRecord::Base.connection.execute 'SELECT pg_sleep(1)'
        end.to raise_error(/canceling statement due to statement timeout/)
        expect do
          ActiveRecord::Base.connection.execute 'SELECT 2'
        end.to raise_error(/current transaction is aborted, commands ignored until end of transaction block/)
      end
      ActiveRecord::Base.connection.execute 'SELECT 1'
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

  describe 'with datadog APM auto-injected sql comments' do
    before do
      conn('pgdog_sharded', false)
      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
      ActiveRecord::Base.connection.execute "/* pgdog_shard: 0 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 0)"
      ActiveRecord::Base.connection.execute "/* pgdog_shard: 1 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 1)"
      ActiveRecord::Base.connection.execute '/* pgdog_shard: 0 */ SELECT pgdog.install_shard_id(0)'
      ActiveRecord::Base.connection.execute '/* pgdog_shard: 1 */ SELECT pgdog.install_shard_id(1)'
    end

    it 'inserts and reads records inside a real trace' do
      # Datadog's :full comments_propagation appends a sqlcommenter trailing
      # comment carrying traceparent, tracestate, dddbs, dde, ddps, ddpv, ddh,
      # and any Rails marginalia metadata. PgDog must strip these cleanly.
      Datadog::Tracing.trace(
        'pgdog.integration.ar',
        service: 'pgdog-ar',
        resource: 'sharded.write',
        tags: {
          'custom.tag' => 'integration',
          'request.id' => SecureRandom.uuid,
          'correlation.id' => SecureRandom.uuid,
          'user.id' => 42,
          'tenant.id' => 'acme',
          'component' => 'active_record'
        }
      ) do |span, trace|
        span.set_tag('trace.id.hex', trace.id.to_s(16))
        span.set_tag('span.id.hex', span.id.to_s(16))

        10.times do |i|
          res = Sharded.create value: "traced_#{i}"
          expect(res.id).to be > 0
          expect(Sharded.find(res.id).value).to eq("traced_#{i}")
        end
      end
    end

    it 'honors leading pgdog_shard directive with datadog trailing comment' do
      Datadog::Tracing.trace('pgdog.shard.route') do |span|
        span.set_tag('expected.shard', 0)
        res = ActiveRecord::Base.connection.execute(
          '/* pgdog_shard: 0 */ SELECT pgdog.shard_id() AS shard'
        )
        expect(res[0]['shard'].to_i).to eq(0)
      end

      Datadog::Tracing.trace('pgdog.shard.route') do |span|
        span.set_tag('expected.shard', 1)
        res = ActiveRecord::Base.connection.execute(
          '/* pgdog_shard: 1 */ SELECT pgdog.shard_id() AS shard'
        )
        expect(res[0]['shard'].to_i).to eq(1)
      end
    end

    it 'honors pgdog_role directive with datadog trailing comment' do
      Datadog::Tracing.trace('pgdog.role.route') do |_span|
        ActiveRecord::Base.connection.execute '/* pgdog_role: primary */ SELECT 1'
        ActiveRecord::Base.connection.execute '/* pgdog_role: replica */ SELECT 1'
      end
    end

    it 'trailing-only pgdog_shard actually routes the query' do
      # No leading directive — PgDog must extract it from the trailing side.
      Datadog::Tracing.trace('pgdog.shard.trailing-only') do |_span|
        res_0 = ActiveRecord::Base.connection.execute(
          'SELECT pgdog.shard_id() AS shard /* pgdog_shard: 0 */'
        )
        expect(res_0[0]['shard'].to_i).to eq(0)

        res_1 = ActiveRecord::Base.connection.execute(
          'SELECT pgdog.shard_id() AS shard /* pgdog_shard: 1 */'
        )
        expect(res_1[0]['shard'].to_i).to eq(1)
      end
    end
  end

  describe 'directive routing enforcement (statistical)' do
    # A pgdog_shard directive that is parsed but ignored would let the query
    # fall through to round-robin across 2 shards — so 20 iterations without
    # a single mismatch gives ~1 - (1/2)^20 ≈ 0.9999990 confidence that the
    # directive is actually being honored, not just stripped.

    before do
      conn('pgdog_sharded', false)
      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
      ActiveRecord::Base.connection.execute "/* pgdog_shard: 0 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 0)"
      ActiveRecord::Base.connection.execute "/* pgdog_shard: 1 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 1)"
      ActiveRecord::Base.connection.execute '/* pgdog_shard: 0 */ SELECT pgdog.install_shard_id(0)'
      ActiveRecord::Base.connection.execute '/* pgdog_shard: 1 */ SELECT pgdog.install_shard_id(1)'
    end

    it 'leading pgdog_shard directive survives 20 iterations per shard' do
      [0, 1].each do |shard|
        20.times do
          res = ActiveRecord::Base.connection.execute(
            "/* pgdog_shard: #{shard} */ SELECT pgdog.shard_id() AS s"
          )
          expect(res[0]['s'].to_i).to eq(shard)
        end
      end
    end

    it 'trailing pgdog_shard directive survives 20 iterations per shard' do
      [0, 1].each do |shard|
        20.times do
          res = ActiveRecord::Base.connection.execute(
            "SELECT pgdog.shard_id() AS s /* pgdog_shard: #{shard} */"
          )
          expect(res[0]['s'].to_i).to eq(shard)
        end
      end
    end

    it 'leading pgdog_shard survives alongside a datadog trailing comment' do
      fake_datadog = "/*dddbs='postgres',dde='prod',ddps='rails',ddpv='1.0',traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/"
      [0, 1].each do |shard|
        20.times do
          res = ActiveRecord::Base.connection.execute(
            "/* pgdog_shard: #{shard} */ SELECT pgdog.shard_id() AS s #{fake_datadog}"
          )
          expect(res[0]['s'].to_i).to eq(shard)
        end
      end
    end

    it 'conflicting directives: leading pgdog_shard wins over trailing' do
      # Proves directive extraction precedence under real routing.
      20.times do
        res = ActiveRecord::Base.connection.execute(
          '/* pgdog_shard: 0 */ SELECT pgdog.shard_id() AS s /* pgdog_shard: 1 */'
        )
        expect(res[0]['s'].to_i).to eq(0)
      end
    end
  end

  describe 'with synthetic tracing comments (no agent required)' do
    before do
      conn('pgdog_sharded', false)
      ActiveRecord::Base.connection.execute 'DROP TABLE IF EXISTS sharded'
      ActiveRecord::Base.connection.execute 'CREATE TABLE sharded (id BIGSERIAL PRIMARY KEY, value TEXT)'
      ActiveRecord::Base.connection.execute "/* pgdog_shard: 0 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 0)"
      ActiveRecord::Base.connection.execute "/* pgdog_shard: 1 */ SELECT pgdog.install_next_id('pgdog', 'sharded', 'id', 2, 1)"
      ActiveRecord::Base.connection.execute '/* pgdog_shard: 0 */ SELECT pgdog.install_shard_id(0)'
      ActiveRecord::Base.connection.execute '/* pgdog_shard: 1 */ SELECT pgdog.install_shard_id(1)'
    end

    # Every tracing ID / correlation ID shape we can think of that a real
    # app might smuggle into a SQL comment. PgDog must strip each of them
    # without breaking an adjacent pgdog_shard directive.
    TRACING_COMMENTS = [
      # W3C trace context, traceparent only
      "/*traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/",
      # W3C traceparent + tracestate
      "/*traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',tracestate='rojo=00f067aa0ba902b7,congo=t61rcWkgMzE'*/",
      # W3C baggage
      "/*baggage='userId=alice,serverNode=DF:28,isProduction=false'*/",
      # Datadog sqlcommenter :service mode
      "/*dddbs='postgres',dde='prod',ddps='my-svc',ddpv='1.0.0'*/",
      # Datadog sqlcommenter :full mode with host
      "/*dddbs='postgres',dde='prod',ddh='db-01',ddps='my-svc',ddpv='1.0.0',traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/",
      # Datadog :full + tracestate carrying dd-specific values
      "/*dddbs='postgres',dde='prod',ddh='db-01',ddps='my-svc',ddpv='1.0.0',traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',tracestate='dd=s:1;t.dm:-0;t.usr.id:abc123'*/",
      # Rails marginalia-style (action + controller) + datadog + traceparent
      "/*action='index',controller='users',dddbs='postgres',dde='prod',ddps='rails-app',ddpv='1.0',traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/",
      # B3 multi-header style squeezed into a comment
      "/*x-b3-traceid='80f198ee56343ba864fe8b2a57d3eff7',x-b3-spanid='e457b5a2e4d86bd1',x-b3-parentspanid='05e3ac9a4f6e3b90',x-b3-sampled='1'*/",
      # B3 single-header style
      "/*b3='80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1-05e3ac9a4f6e3b90'*/",
      # Common correlation / request IDs
      "/*request_id='550e8400-e29b-41d4-a716-446655440000',correlation_id='abc-123-def-456-ghi-789'*/",
      # AWS X-Ray trace header
      "/*x-amzn-trace-id='Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1'*/",
      # GCP Cloud Trace
      "/*x-cloud-trace-context='105445aa7843bc8bf206b12000100000/1;o=1'*/",
      # OpenTelemetry-style with multiple propagators at once
      "/*traceparent='00-abc123abc123abc123abc123abc12345-12345678abcdef00-01',tracestate='vendorname1=opaqueValue1,vendorname2=opaqueValue2',baggage='key1=value1'*/",
      # High-cardinality custom tags — quoted strings with spaces
      '/*user_email="alice@example.com",session_id="sess_01HX7Y8K9MNPQRSTUVWXYZ0123",feature_flag="enabled"*/',
      # Many consecutive comments (some tools stack them)
      "/*dddbs='postgres'*/ /*traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/"
    ].freeze

    TRACING_COMMENTS.each_with_index do |tcomment, idx|
      it "routes pgdog_shard correctly with tracing comment ##{idx}" do
        sql_0 = "/* pgdog_shard: 0 */ SELECT pgdog.shard_id() AS shard #{tcomment}"
        sql_1 = "/* pgdog_shard: 1 */ SELECT pgdog.shard_id() AS shard #{tcomment}"

        res_0 = ActiveRecord::Base.connection.execute(sql_0)
        res_1 = ActiveRecord::Base.connection.execute(sql_1)
        expect(res_0[0]['shard'].to_i).to eq(0)
        expect(res_1[0]['shard'].to_i).to eq(1)
      end

      it "inserts and reads with tracing comment ##{idx}" do
        ins = "INSERT INTO sharded (value) VALUES ('trace_#{idx}') RETURNING id #{tcomment}"
        sel = "SELECT value FROM sharded WHERE value = 'trace_#{idx}' #{tcomment}"

        ins_res = ActiveRecord::Base.connection.execute(ins)
        expect(ins_res.num_tuples).to eq(1)

        sel_res = ActiveRecord::Base.connection.execute(sel).to_a
        expect(sel_res.size).to eq(1)
        expect(sel_res[0]['value']).to eq("trace_#{idx}")
      end
    end

    it 'extracts pgdog_sharding_key from a stack of tracing comments' do
      # This config has multiple sharding functions, so a bare
      # pgdog_sharding_key directive provokes a downstream error — but only
      # if the comment parser actually saw the directive. Proving the error
      # reaches us proves the parser stripped the surrounding tracing noise
      # and extracted the directive.
      tcomment = TRACING_COMMENTS[4] # full datadog comment
      expect do
        ActiveRecord::Base.connection.execute(
          "/* pgdog_sharding_key: 1 */ SELECT 1 #{tcomment}"
        )
      end.to raise_error(/sharding function/)
      # Leave the connection in a clean state for ensure_done.
      ActiveRecord::Base.connection.reconnect!
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
            rescue StandardError
              # Killed mid-query or other error
              mutex.synchronize { interrupted_count += 1 }
            end
          end
          chaos_threads << t
        end

        # Start killer thread that randomly kills query threads
        killer = Thread.new do
          50.times do
            sleep(rand(0.01..0.05))
            alive_threads = chaos_threads.select(&:alive?)
            next unless alive_threads.any?

            victim = alive_threads.sample
            victim.kill
            mutex.synchronize { interrupted_count += 1 }
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
