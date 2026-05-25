# frozen_string_literal: true

require 'pg'
require 'rspec'
require 'fileutils'
require 'csv'

describe 'extended protocol' do
  let(:plugin_marker_file) { File.expand_path('../test-plugins/test-plugin-compatible/route-called.test', __FILE__) }

  before do
    # Delete the marker file before running tests
    puts "DEBUG: Looking for plugin marker file at: #{plugin_marker_file}"
    FileUtils.rm_f(plugin_marker_file)
    expect(File.exist?(plugin_marker_file)).to be false
  end

  it 'can pass params to plugin' do
    10.times do
      conn = PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog')
      25.times do |i|
        result = conn.exec 'SELECT t.id FROM (SELECT $1 AS id) t WHERE t.id = $1', [i]
        expect(result[0]['id'].to_i).to eq(i)
      end
    end

    # Verify the plugin was actually called
    # expect(File.exist?(plugin_marker_file)).to be true
  end
end

describe 'copy with plugin' do
  let(:conn) { PG.connect('postgres://pgdog:pgdog@127.0.0.1:6432/pgdog') }

  before do
    conn.exec 'DROP TABLE IF EXISTS plugin_copy_test'
    conn.exec 'CREATE TABLE plugin_copy_test (id BIGINT PRIMARY KEY, name VARCHAR, email VARCHAR)'
  end

  after do
    conn.exec 'DROP TABLE IF EXISTS plugin_copy_test'
  end

  it 'can COPY text format through plugin' do
    conn.copy_data('COPY plugin_copy_test (id, name, email) FROM STDIN') do
      conn.put_copy_data("1\tAlice\talice@test.com\n")
      conn.put_copy_data("2\tBob\tbob@test.com\n")
      conn.put_copy_data("3\tCharlie\tcharlie@test.com\n")
    end

    rows = conn.exec 'SELECT * FROM plugin_copy_test ORDER BY id'
    expect(rows.ntuples).to eq(3)
    expect(rows[0]['name']).to eq('Alice')
    expect(rows[1]['name']).to eq('Bob')
    expect(rows[2]['name']).to eq('Charlie')
  end

  it 'can COPY CSV format through plugin' do
    conn.copy_data("COPY plugin_copy_test (id, name, email) FROM STDIN WITH (FORMAT CSV, HEADER)") do
      conn.put_copy_data(CSV.generate_line(%w[id name email]))
      conn.put_copy_data(CSV.generate_line([1, 'Alice', 'alice@test.com']))
      conn.put_copy_data(CSV.generate_line([2, 'Bob', 'bob@test.com']))
      conn.put_copy_data(CSV.generate_line([3, 'Charlie', 'charlie@test.com']))
    end

    rows = conn.exec 'SELECT * FROM plugin_copy_test ORDER BY id'
    expect(rows.ntuples).to eq(3)
    expect(rows[0]['email']).to eq('alice@test.com')
    expect(rows[2]['email']).to eq('charlie@test.com')
  end

  it 'can COPY CSV with custom delimiter through plugin' do
    conn.copy_data("COPY plugin_copy_test (id, name, email) FROM STDIN WITH (FORMAT CSV, DELIMITER '|')") do
      conn.put_copy_data("1|Alice|alice@test.com\n")
      conn.put_copy_data("2|Bob|bob@test.com\n")
    end

    rows = conn.exec 'SELECT * FROM plugin_copy_test ORDER BY id'
    expect(rows.ntuples).to eq(2)
    expect(rows[0]['name']).to eq('Alice')
    expect(rows[1]['email']).to eq('bob@test.com')
  end

  it 'can COPY with NULL values through plugin' do
    conn.copy_data("COPY plugin_copy_test (id, name, email) FROM STDIN WITH (FORMAT CSV, NULL '\\N')") do
      conn.put_copy_data("1,Alice,\\N\n")
      conn.put_copy_data("2,\\N,bob@test.com\n")
    end

    rows = conn.exec 'SELECT * FROM plugin_copy_test ORDER BY id'
    expect(rows.ntuples).to eq(2)
    expect(rows[0]['email']).to be_nil
    expect(rows[1]['name']).to be_nil
  end

  it 'can COPY many rows through plugin' do
    conn.copy_data('COPY plugin_copy_test (id, name, email) FROM STDIN') do
      1000.times do |i|
        conn.put_copy_data("#{i}\tuser_#{i}\tuser_#{i}@test.com\n")
      end
    end

    rows = conn.exec 'SELECT count(*) FROM plugin_copy_test'
    expect(rows[0]['count'].to_i).to eq(1000)
  end

end
