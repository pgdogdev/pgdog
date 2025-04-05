# frozen_string_literal: true

require_relative 'rspec_helper'

def connect(dbname = 'pgdog')
  PG.connect(dbname: dbname, user: 'pgdog', password: 'pgdog', port: 6432, host: '127.0.0.1')
end

describe 'pg' do
  after do
    ensure_done
  end

  it 'simple query' do
    %w[pgdog pgdog_sharded].each do |db|
      conn = connect db
      res = conn.exec 'SELECT 1::bigint AS one'
      expect(res[0]['one']).to eq('1')
      res = conn.exec 'SELECT $1 AS one, $2 AS two', [1, 2]
      expect(res[0]['one']).to eq('1')
      expect(res[0]['two']).to eq('2')
    end
  end

  it 'prepared statements' do
    %w[pgdog pgdog_sharded].each do |db|
      conn = connect db
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

  it 'sharded' do
    conn = connect 'pgdog_sharded'
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

  it 'transactions' do
    %w[pgdog pgdog_sharded].each do |db|
      conn = connect db
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
