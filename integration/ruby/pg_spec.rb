# frozen_string_literal: true

require_relative 'rspec_helper'

PIPELINE_RESULT_COUNT = 5
PIPELINE_RESULT_TIMEOUT = 5

def connect(dbname = 'pgdog', user = 'pgdog', port: 6432)
  PG.connect(dbname: dbname, user: user, password: 'pgdog', port: port, host: '127.0.0.1')
end

def connect_direct(dbname = 'pgdog', user = 'pgdog')
  connect(dbname, user, port: 5432)
end

def drain_pipeline_results(conn)
  results = []

  while results.length < PIPELINE_RESULT_COUNT
    unless conn.block(PIPELINE_RESULT_TIMEOUT)
      raise "timed out waiting for pipeline results: #{results.inspect}, pipeline_status=#{conn.pipeline_status}"
    end

    result = conn.sync_get_result
    next if result.nil?

    trace = {
      status: result.res_status,
      cmd_status: result.cmd_status
    }

    if result.result_status == PG::PGRES_TUPLES_OK
      trace[:values] = result.values
    end

    error = result.error_message
    trace[:error] = error unless error.nil? || error.empty?

    results << trace
  end

  results
end

def run_named_prepare_pipeline(conn, name, value)
  conn.enter_pipeline_mode

  conn.send_query_params('BEGIN', [])
  conn.send_prepare(name, 'SELECT $1::bigint AS one')
  conn.send_query_prepared(name, [value])
  conn.send_query_params('COMMIT', [])
  conn.pipeline_sync

  results = drain_pipeline_results(conn)
  conn.exit_pipeline_mode
  results
ensure
  begin
    conn.discard_results
  rescue PG::Error
    nil
  end

  begin
    conn.exit_pipeline_mode
  rescue PG::Error
    nil
  end
end

def expect_named_prepare_pipeline_trace(trace, value)
  expect(trace.map { |result| result[:status] }).to eq([
    'PGRES_COMMAND_OK',
    'PGRES_COMMAND_OK',
    'PGRES_TUPLES_OK',
    'PGRES_COMMAND_OK',
    'PGRES_PIPELINE_SYNC'
  ])

  tuple_results = trace.select { |result| result[:status] == 'PGRES_TUPLES_OK' }

  expect(tuple_results).to eq([
    {
      status: 'PGRES_TUPLES_OK',
      cmd_status: 'SELECT 1',
      values: [[value.to_s]]
    }
  ])
end

describe 'pg' do
  after do
    ensure_done
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

  it 'matches direct postgres for a pipelined extended transaction with a named prepared statement' do
    direct = connect_direct
    proxied = connect
    value = 42

    begin
      direct_trace = run_named_prepare_pipeline(direct, 'pipeline_stmt_direct', value)
      proxied_trace = run_named_prepare_pipeline(proxied, 'pipeline_stmt_pgdog', value)

      expect_named_prepare_pipeline_trace(direct_trace, value)
      expect(proxied_trace).to eq(direct_trace)

      expect(direct.exec('SELECT 1::bigint AS one')[0]['one']).to eq('1')
      expect(proxied.exec('SELECT 1::bigint AS one')[0]['one']).to eq('1')
    ensure
      direct.close
      proxied.close
    end
  end
end
