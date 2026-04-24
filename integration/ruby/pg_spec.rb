# frozen_string_literal: true

require_relative 'rspec_helper'

def connect(dbname = 'pgdog', user = 'pgdog')
  PG.connect(dbname: dbname, user: user, password: 'pgdog', port: 6432, host: '127.0.0.1', application_name: '')
end

describe 'pg' do
  after do
    sleep(1)
    ensure_done
  end

  it 'out of sync' do
    conn = connect 'pgdog', 'pgdog'
    conn.exec_params 'SELECT $1', [1]
    conn.exec 'SELECT 1'
    conn.exec 'SET lock_timeout TO sdfs'
    conn.exec "SET statement_timeout TO '1s'"
    expect { conn.exec 'SELECT 1' }.to raise_error(/invalid value for parameter/)
  end

  it 'simple query' do
    %w[pgdog pgdog_sharded].each do |db|
      %w[pgdog pgdog_2pc].each do |user|
        conn = connect db, user
        res = conn.exec 'SELECT 1::bigint AS one'
        expect(res[0]['one']).to eq('1')
        res = conn.exec 'SELECT $1 AS one, $2 AS two', [1, 2]
        expect(res[0]['one']).to eq('1')
        expect(res[0]['two']).to eq('2')
      end
    end
  end

  it 'prepared statements' do
    %w[pgdog pgdog_sharded].each do |db|
      %w[pgdog pgdog_2pc].each do |user|
        conn = connect db, user
        15.times do |i|
          name = "_pg_#{i}"
          conn.prepare name, 'SELECT $1 AS one'
          res = conn.exec_prepared name, [i]
          expect(res[0]['one']).to eq(i.to_s)
        end
        30.times do |_i|
          15.times do |i|
            name = "_pg_#{i}"
            res = conn.exec_prepared name, [i]
            expect(res[0]['one']).to eq(i.to_s)
          end
        end
      end
    end
  end

  it 'sharded' do
    %w[pgdog pgdog_2pc].each do |user|
      conn = connect 'pgdog_sharded', user
      conn.exec 'DROP TABLE IF EXISTS sharded'
      conn.exec 'CREATE TABLE sharded (id BIGINT, value TEXT)'
      conn.prepare 'insert', 'INSERT INTO sharded (id, value) VALUES ($1, $2) RETURNING *'
      conn.prepare 'select', 'SELECT * FROM sharded WHERE id = $1'
      15.times do |i|
        [10, 10_000_000_000].each do |num|
          id = num + i
          results = []
          results << conn.exec('INSERT INTO sharded (id, value) VALUES ($1, $2) RETURNING *', [id, 'value_one'])
          results << conn.exec('SELECT * FROM sharded WHERE id = $1', [id])
          results.each do |result|
            expect(result.num_tuples).to eq(1)
            expect(result[0]['id']).to eq(id.to_s)
            expect(result[0]['value']).to eq('value_one')
          end
          conn.exec 'TRUNCATE TABLE sharded'
          results << conn.exec_prepared('insert', [id, 'value_one'])
          results << conn.exec_prepared('select', [id])
          results.each do |result|
            expect(result.num_tuples).to eq(1)
            expect(result[0]['id']).to eq(id.to_s)
            expect(result[0]['value']).to eq('value_one')
          end
        end
      end
    end
  end

  it 'transactions' do
    %w[pgdog pgdog_sharded].each do |db|
      %w[pgdog pgdog_2pc].each do |user|
        conn = connect db, user
        conn.exec 'DROP TABLE IF EXISTS sharded'
        conn.exec 'CREATE TABLE sharded (id BIGINT, value TEXT)'
        conn.prepare 'insert', 'INSERT INTO sharded (id, value) VALUES ($1, $2) RETURNING *'
        conn.prepare 'select', 'SELECT * FROM sharded WHERE id = $1'

        conn.exec 'BEGIN'
        res = conn.exec_prepared 'insert', [1, 'test']
        conn.exec 'COMMIT'
        expect(res.num_tuples).to eq(1)
        expect(res[0]['id']).to eq(1.to_s)
        conn.exec 'BEGIN'
        res = conn.exec_prepared 'select', [1]
        expect(res.num_tuples).to eq(1)
        expect(res[0]['id']).to eq(1.to_s)
        conn.exec 'ROLLBACK'
        conn.exec 'SELECT 1'
      end
    end
  end

  it 'session_control_and_locks query parser' do
    a = admin
    a.exec "SET query_parser TO 'session_control_and_locks'"
    begin
      conn = connect 'pgdog'
      res = conn.exec 'SELECT pg_advisory_lock(42)'
      expect(res.num_tuples).to eq(1)
      res = conn.exec 'SELECT pg_advisory_unlock(42)'
      expect(res[0]['pg_advisory_unlock']).to eq('t')
      conn.exec 'BEGIN'
      conn.exec 'SELECT pg_advisory_lock(99)'
      conn.exec 'SELECT pg_advisory_unlock(99)'
      conn.exec 'COMMIT'
      conn.exec 'SELECT 1'
      conn.close
    ensure
      a.exec "SET query_parser TO 'auto'"
      a.close
    end
  end

  it 'session_control_and_locks concurrent advisory locks' do
    a = admin
    a.exec "SET query_parser TO 'session_control_and_locks'"
    begin
      ids = (1..16).map { rand(1..2_000_000_000) }.uniq
      threads = ids.map do |lock_id|
        Thread.new do
          conn = connect 'pgdog'
          10.times do
            res = conn.exec_params 'SELECT pg_try_advisory_lock($1)', [lock_id]
            expect(res[0]['pg_try_advisory_lock']).to eq('t')
            sleep(rand * 0.01)
            res = conn.exec_params 'SELECT pg_advisory_unlock($1)', [lock_id]
            expect(res[0]['pg_advisory_unlock']).to eq('t')
          end
          conn.close
        end
      end
      threads.each(&:join)
    ensure
      a.exec "SET query_parser TO 'auto'"
      a.close
    end
  end

  it 'session_control_and_locks SET persists session state' do
    a = admin
    a.exec "SET query_parser TO 'session_control_and_locks'"
    begin
      conn = connect 'pgdog'
      conn.exec "SET statement_timeout TO '7531ms'"
      res = conn.exec 'SHOW statement_timeout'
      expect(res[0]['statement_timeout']).to eq('7531ms')
      5.times do
        res = conn.exec 'SHOW statement_timeout'
        expect(res[0]['statement_timeout']).to eq('7531ms')
      end

      conn.exec "SET application_name TO 'pgdog_session_test'"
      res = conn.exec 'SHOW application_name'
      expect(res[0]['application_name']).to eq('pgdog_session_test')

      conn.exec 'RESET statement_timeout'
      res = conn.exec 'SHOW statement_timeout'
      expect(res[0]['statement_timeout']).not_to eq('7531ms')

      # Extended protocol (Parse/Bind/Execute) must see the same session.
      conn.exec "SET statement_timeout TO '4242ms'"
      res = conn.exec_params 'SELECT current_setting($1)', ['statement_timeout']
      expect(res[0]['current_setting']).to eq('4242ms')
      res = conn.exec_params 'SELECT $1::int + $2::int AS sum', [21, 21]
      expect(res[0]['sum']).to eq('42')
      res = conn.exec_params 'SELECT pg_advisory_lock($1)', [12_345]
      expect(res.num_tuples).to eq(1)
      res = conn.exec_params 'SELECT pg_advisory_unlock($1)', [12_345]
      expect(res[0]['pg_advisory_unlock']).to eq('t')
      res = conn.exec_params 'SELECT current_setting($1)', ['statement_timeout']
      expect(res[0]['current_setting']).to eq('4242ms')
      conn.close

      # Concurrent clients each keep their own session state.
      # Use ms values not divisible by 1000 so Postgres doesn't normalize "1000ms" -> "1s".
      threads = (0...8).map do |i|
        Thread.new do
          c = connect 'pgdog'
          timeout = "#{1001 + i * 113}ms"
          name = "session_thread_#{i}_#{rand(1_000_000)}"
          c.exec "SET statement_timeout TO '#{timeout}'"
          c.exec "SET application_name TO '#{name}'"
          5.times do
            r = c.exec 'SHOW statement_timeout'
            expect(r[0]['statement_timeout']).to eq(timeout)
            r = c.exec_params 'SELECT current_setting($1)', ['application_name']
            expect(r[0]['current_setting']).to eq(name)
          end
          c.close
        end
      end
      threads.each(&:join)
    ensure
      a.exec "SET query_parser TO 'auto'"
      a.close
    end
  end

  it 'deallocate ignored' do
    %w[pgdog pgdog_sharded].each do |db|
      conn = connect db
      conn.prepare 'deallocate_ignored', 'SELECT $1 AS one'
      res = conn.exec_prepared 'deallocate_ignored', [1]
      expect(res[0]['one']).to eq('1')
      conn.exec 'DEALLOCATE deallocate_ignored' # Ignored
      res = conn.exec_prepared 'deallocate_ignored', [2]
      expect(res[0]['one']).to eq('2')
    end
  end
end
