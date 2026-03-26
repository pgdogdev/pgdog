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

describe 'ddl-only mirror' do
  conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')
  ddl_mirror = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_mirror2')

  before do
    conn.exec 'DROP TABLE IF EXISTS ddl_mirror_test'
    ddl_mirror.exec 'DROP TABLE IF EXISTS ddl_mirror_test'
  end

  it 'replicates DDL to ddl-only mirror' do
    conn.exec 'CREATE TABLE ddl_mirror_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    sleep(0.5)

    # DDL should be mirrored
    result = ddl_mirror.exec "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ddl_mirror_test')"
    expect(result[0]['exists']).to eq('t')
  end

  it 'does not replicate DML to ddl-only mirror' do
    conn.exec 'CREATE TABLE ddl_mirror_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    sleep(0.5)

    conn.exec "INSERT INTO ddl_mirror_test VALUES (1, 'should not mirror')"
    sleep(0.5)

    # Table should exist on ddl mirror (DDL was mirrored)
    result = ddl_mirror.exec 'SELECT count(*) FROM ddl_mirror_test'
    # But no rows (DML was not mirrored)
    expect(result[0]['count'].to_i).to eq(0)
  end

  it 'does not replicate UPDATE to ddl-only mirror' do
    conn.exec 'CREATE TABLE ddl_mirror_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    sleep(0.5)

    # Insert directly into ddl mirror so we can check UPDATE doesn't propagate
    ddl_mirror.exec "INSERT INTO ddl_mirror_test VALUES (1, 'original')"

    conn.exec "INSERT INTO ddl_mirror_test VALUES (1, 'original')"
    conn.exec "UPDATE ddl_mirror_test SET value = 'updated' WHERE id = 1"
    sleep(0.5)

    result = ddl_mirror.exec 'SELECT value FROM ddl_mirror_test WHERE id = 1'
    expect(result[0]['value']).to eq('original')
  end

  it 'replicates ALTER TABLE to ddl-only mirror' do
    conn.exec 'CREATE TABLE ddl_mirror_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    sleep(0.5)

    conn.exec 'ALTER TABLE ddl_mirror_test ADD COLUMN extra TEXT'
    sleep(0.5)

    result = ddl_mirror.exec "SELECT column_name FROM information_schema.columns WHERE table_name = 'ddl_mirror_test' AND column_name = 'extra'"
    expect(result.ntuples).to eq(1)
  end

  it 'replicates DROP TABLE to ddl-only mirror' do
    conn.exec 'CREATE TABLE ddl_mirror_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    sleep(0.5)

    result = ddl_mirror.exec "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ddl_mirror_test')"
    expect(result[0]['exists']).to eq('t')

    conn.exec 'DROP TABLE ddl_mirror_test'
    sleep(0.5)

    result = ddl_mirror.exec "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ddl_mirror_test')"
    expect(result[0]['exists']).to eq('f')
  end

  after do
    conn.exec 'DROP TABLE IF EXISTS ddl_mirror_test'
    ddl_mirror.exec 'DROP TABLE IF EXISTS ddl_mirror_test'
  end
end
