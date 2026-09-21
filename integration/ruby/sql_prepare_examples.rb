# frozen_string_literal: true

shared_examples 'SQL PREPARE over extended protocol' do |user|
  it 'executes SQL PREPARE and EXECUTE through named statements' do
    conn = connect('pgdog', user)
    name = "sql_extended_#{SecureRandom.hex(6)}"
    conn.prepare('prepare_command', "PREPARE #{name} AS SELECT $1::bigint * 2 AS val")
    expect(conn.exec_prepared('prepare_command', []).cmd_status).to eq('PREPARE')
    conn.prepare('execute_command', "EXECUTE #{name}(21)")
    3.times do
      expect(conn.exec_prepared('execute_command', [])[0]['val'].to_i).to eq(42)
    end
    expect(conn.exec('SELECT 1')[0].values).to eq(['1'])
  ensure
    conn.close if conn && !conn.finished?
  end

  it 'executes SQL PREPARE and EXECUTE through unnamed statements' do
    conn = connect('pgdog', user)
    name = "sql_unnamed_#{SecureRandom.hex(6)}"
    expect(conn.exec_params("PREPARE #{name} AS SELECT $1::bigint * 2 AS val", []).cmd_status).to eq('PREPARE')
    3.times do
      expect(conn.exec_params("EXECUTE #{name}(21)", [])[0]['val'].to_i).to eq(42)
    end
    expect(conn.exec('SELECT 1')[0].values).to eq(['1'])
  ensure
    conn.close if conn && !conn.finished?
  end
end
