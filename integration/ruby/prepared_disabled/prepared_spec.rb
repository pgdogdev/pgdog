# frozen_string_literal: true

require_relative '../rspec_helper'

# With prepared_statements = "disabled" pgdog forwards protocol messages as-is
# without rewriting or caching.
describe 'prepared_statements = disabled' do
  after { ensure_done }

  # Anonymous statements (empty name) are a single Parse+Bind+Execute+Sync
  # cycle on one backend — no state needs to survive across cycles.
  it 'executes anonymous parameterized queries' do
    conn = connect
    10.times do |i|
      res = conn.exec_params('SELECT $1::bigint * 2 AS val', [i])
      expect(res[0]['val'].to_i).to eq(i * 2)
    end
    conn.close
  end

  # Session mode pins one backend for the connection lifetime; pass-through
  # is sufficient because prepare and execute always reach the same backend.
  it 'passes named statements through in session mode' do
    conn = connect('pgdog', 'pgdog_session')
    conn.prepare('session_stmt', 'SELECT $1::bigint AS val')
    10.times do |i|
      res = conn.exec_prepared('session_stmt', [i])
      expect(res[0]['val'].to_i).to eq(i)
    end
    conn.close
  end

  # Session mode gives each client its own dedicated backend, so two session
  # connections are guaranteed to land on different backends. Without a global
  # cache the execute on conn2 reaches a backend that never saw the prepare.
  it 'does not share statements across connections' do
    conn1 = connect('pgdog', 'pgdog_session')
    conn2 = connect('pgdog', 'pgdog_session')
    conn1.prepare('cross_stmt', 'SELECT $1::bigint AS val')
    expect do
      conn2.exec_prepared('cross_stmt', [7])
    end.to raise_error(PG::Error)
    conn1.close
    conn2.close
  end

  # Prepare on backend A. pgdog intercepts bare BEGIN without checking out a
  # backend (deferred until the first real query), so a SELECT 1 is needed to
  # force the actual checkout and keep backend A pinned in the open transaction.
  # exec_prepared then lands on backend B which never saw the Parse.
  # In disabled mode pgdog forwards Bind as-is — 'prepared statement does not exist'.
  #
  # Mirror: full/'executes named extended-protocol statements in
  # transaction pool mode' — identical structure, opposite expectation.
  it 'fails named extended-protocol statements in transaction pool mode' do
    conn = connect
    begin
      conn.prepare('ext_stmt', 'SELECT $1::bigint AS val')
      pin = connect
      begin
        pin.exec('BEGIN')
        pin.exec('SELECT 1') # force actual pool checkout; pin now holds backend A
        expect { conn.exec_prepared('ext_stmt', [42]) }.to raise_error(PG::Error)
      ensure
        pin.exec('ROLLBACK') rescue nil
        pin.close rescue nil
      end
    ensure
      conn.close rescue nil
    end
  end


  # Same mechanism as the extended-protocol test above, but for the
  # simple-protocol PREPARE/EXECUTE path. disabled mode forwards the SQL
  # statement text as-is, so EXECUTE on a backend that never saw the
  # PREPARE fails with 'prepared statement does not exist' or, if two
  # threads hit the same backend, 'already exists'.
  #
  # Mirror: full/'rewrites simple-protocol PREPARE / EXECUTE in
  # transaction pool mode' — same structure, opposite expectation.
  it 'fails SQL PREPARE/EXECUTE in transaction pool mode' do
    errors = []
    mutex  = Mutex.new

    threads = 5.times.map do
      Thread.new do
        conn = connect
        begin
          conn.exec('PREPARE sql_stmt AS SELECT $1::bigint * 2')
          20.times { |i| conn.exec("EXECUTE sql_stmt(#{i})") }
        rescue PG::Error => e
          mutex.synchronize { errors << e.message }
        ensure
          conn.close rescue nil
        end
      end
    end

    threads.each(&:join)
    expect(errors).not_to be_empty
  end
end
