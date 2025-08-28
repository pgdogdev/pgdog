# frozen_string_literal: true

require 'rspec'
require 'pg'
require 'csv'

describe 'mirror copy' do
  conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')

  before do
    # Clean up any existing tables first in both databases
    conn.exec 'DROP TABLE IF EXISTS mirror_copy_test'
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    mirror_direct.exec 'DROP TABLE IF EXISTS mirror_copy_test'
    mirror_direct.close
    
    # Create table through PgDog - it will be mirrored automatically
    conn.exec 'BEGIN'
    conn.exec 'CREATE TABLE mirror_copy_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    conn.exec 'COMMIT'
    # Wait for mirror to create the table
    sleep(0.5)
  end

  it 'can copy CSV' do
    # Start a transaction to ensure mirror captures it
    conn.exec 'BEGIN'
    
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
    
    # Commit to trigger mirror flush
    conn.exec 'COMMIT'

    # Wait for mirror to process
    sleep(1.0)
    # Connect directly to the mirror database to verify replication
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    rows = mirror_direct.exec 'SELECT * FROM mirror_copy_test'
    expect(rows.ntuples).to eq(3)
    mirror_direct.close
  end

  after do
    conn.exec 'DROP TABLE IF EXISTS mirror_copy_test'
    # Clean up mirror database too
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    mirror_direct.exec 'DROP TABLE IF EXISTS mirror_copy_test'
    mirror_direct.close
  end
end

describe 'mirror crud' do
  conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')

  before do
    # Clean up any existing tables first in both databases
    conn.exec 'DROP TABLE IF EXISTS mirror_crud_test'
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    mirror_direct.exec 'DROP TABLE IF EXISTS mirror_crud_test'
    mirror_direct.close
    
    # Create table through PgDog - it will be mirrored automatically
    conn.exec 'BEGIN'
    conn.exec 'CREATE TABLE mirror_crud_test (id BIGINT PRIMARY KEY, value VARCHAR)'
    conn.exec 'COMMIT'
    # Wait for mirror to create the table
    sleep(0.5)
    
    conn.prepare 'insert', 'INSERT INTO mirror_crud_test VALUES ($1, $2) RETURNING *'
  end

  it 'can insert rows' do
    # Start a transaction to ensure mirror captures it
    conn.exec 'BEGIN'
    results = conn.exec_prepared 'insert', [1, 'hello world']
    expect(results.ntuples).to eq(1)
    results = conn.exec_prepared 'insert', [2, 'apples and oranges']
    expect(results.ntuples).to eq(1)
    # Commit to trigger mirror flush
    conn.exec 'COMMIT'

    # Wait for mirror to process
    sleep(1.0)

    # Connect directly to the mirror database to verify replication
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    results = mirror_direct.exec 'SELECT * FROM mirror_crud_test WHERE id = $1 AND value = $2', [1, 'hello world']
    expect(results.ntuples).to eq(1)
    mirror_direct.close
  end

  it 'can update rows' do
    # First transaction for insert
    conn.exec 'BEGIN'
    conn.exec "INSERT INTO mirror_crud_test VALUES (3, 'update me')"
    conn.exec 'COMMIT'
    sleep(1.0)
    # Connect directly to the mirror database to verify replication
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    result = mirror_direct.exec 'SELECT * FROM mirror_crud_test WHERE id = 3'
    expect(result.ntuples).to eq(1)
    expect(result[0]['value']).to eq('update me')
    mirror_direct.close
    
    # Second transaction for update
    conn.exec 'BEGIN'
    conn.exec 'UPDATE mirror_crud_test SET value = $1 WHERE id = $2', ['updated value', 3]
    conn.exec 'COMMIT'
    sleep(1.0)
    
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    result = mirror_direct.exec 'SELECT * FROM mirror_crud_test WHERE id = 3'
    expect(result[0]['value']).to eq('updated value')
    mirror_direct.close
  end

  after do
    conn.exec 'DROP TABLE IF EXISTS mirror_crud_test'
    # Clean up mirror database too
    mirror_direct = PG.connect('postgres://pgdog:pgdog@127.0.0.1:5432/pgdog1')
    mirror_direct.exec 'DROP TABLE IF EXISTS mirror_crud_test'
    mirror_direct.close
  end
end
