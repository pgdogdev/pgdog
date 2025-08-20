# frozen_string_literal: true

require 'rspec'
require 'pg'
require 'csv'

describe 'mirror copy' do
  conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')
  mirror = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_mirror')

  before do
    conn.exec 'DROP TABLE IF EXISTS mirror_copy_test'
    conn.exec 'CREATE TABLE mirror_copy_test (id BIGINT PRIMARY KEY, value VARCHAR)'
  end

  it 'can copy CSV' do
    conn.copy_data("COPY mirror_copy_test (id, value) FROM STDIN WITH (FORMAT CSV, NULL '\\N', HEADER)") do
      rows = [
        %w[id value],
        %w[1 hello@test.com],
        [2, 'longer text in quotes'],
        [3, nil]
      ]

      rows.each do |row|
        conn.put_copy_data(CSV.generate_line(row, force_quotes: true, nil_value: '\\N'))
      end
    end

    rows = conn.exec 'SELECT * FROM mirror_copy_test'
    expect(rows.ntuples).to eq(3)

    # Wait for mirror flush.
    sleep(0.5)
    rows = mirror.exec 'SELECT * FROM mirror_copy_test'
    expect(rows.ntuples).to eq(3)
  end

  after do
    conn.exec 'DROP TABLE IF EXISTS mirror_copy_test'
  end
end

describe 'mirror crud' do
  conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')
  mirror = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_mirror')

  before do
    conn.exec 'DROP TABLE IF EXISTS mirror_crud_test'
    conn.exec 'CREATE TABLE mirror_crud_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    conn.prepare 'insert', 'INSERT INTO mirror_crud_test VALUES ($1, $2) RETURNING *'
  end

  it 'can insert rows' do
    results = conn.exec_prepared 'insert', [1, 'hello world']
    expect(results.ntuples).to eq(1)
    results = conn.exec_prepared 'insert', [2, 'apples and oranges']
    expect(results.ntuples).to eq(1)

    # Wait for mirror flush
    sleep(0.5)

    results = mirror.exec 'SELECT * FROM mirror_crud_test WHERE id = $1 AND value = $2', [1, 'hello world']
    expect(results.ntuples).to eq(1)
  end

  it 'can update rows' do
    conn.exec "INSERT INTO mirror_crud_test VALUES (3, 'update me')"
    sleep(0.5)
    result = mirror.exec 'SELECT * FROM mirror_crud_test WHERE id = 3'
    expect(result.ntuples).to eq(1)
    expect(result[0]['value']).to eq('update me')
    conn.exec 'UPDATE mirror_crud_test SET value = $1 WHERE id = $2', ['updated value', 3]
    sleep(0.5)
    result = mirror.exec 'SELECT * FROM mirror_crud_test WHERE id = 3'
    expect(result[0]['value']).to eq('updated value')
  end

  after do
    conn.exec 'DROP TABLE IF EXISTS mirror_copy_test'
  end
end
