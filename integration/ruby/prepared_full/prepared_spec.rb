# frozen_string_literal: true

require_relative '../rspec_helper'

describe 'prepared_statements = full' do
  after { ensure_done }

  # Mirror of disabled suite: anonymous statements carry no per-backend state,
  # so they work identically regardless of the prepared_statements setting.
  it 'executes anonymous parameterized queries' do
    conn = connect
    10.times do |i|
      res = conn.exec_params('SELECT $1::bigint * 2 AS val', [i])
      expect(res[0]['val'].to_i).to eq(i * 2)
    end
    conn.close
  end

  # Mirror of disabled suite: session mode pins the client to one backend for
  # the connection lifetime, so named statements always reach the backend that
  # holds them regardless of the prepared_statements setting.
  it 'passes named statements in session mode' do
    conn = connect('pgdog', 'pgdog_session')
    conn.prepare('session_stmt', 'SELECT $1::bigint AS val')
    10.times do |i|
      res = conn.exec_prepared('session_stmt', [i])
      expect(res[0]['val'].to_i).to eq(i)
    end
    conn.close
  end

  # Mirror of disabled/'fails SQL PREPARE/EXECUTE in transaction pool mode'.
  # full mode intercepts PREPARE, renames the statement to an internal name
  # (__pgdog_N), and replays the PREPARE on any backend that hasn't seen it
  # before executing. 5 threads × 20 iterations with pool_size = 2 generates
  # the same backend crossings as the disabled test, but all succeed.
  it 'rewrites simple-protocol PREPARE / EXECUTE in transaction pool mode' do
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
    expect(errors).to be_empty
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

  # Mirror of disabled/'fails named extended-protocol statements in
  # transaction pool mode' — identical structure, opposite expectation.
  # pgdog intercepts bare BEGIN without a pool checkout, so SELECT 1 forces
  # the actual checkout keeping backend A pinned. exec_prepared lands on
  # backend B; full mode replays the Parse from the global cache — succeeds.
  it 'executes named extended-protocol statements in transaction pool mode' do
    conn = connect
    begin
      conn.prepare('ext_stmt', 'SELECT $1::bigint AS val')
      pin = connect
      begin
        pin.exec('BEGIN')
        pin.exec('SELECT 1') # force actual pool checkout; pin now holds backend A
        10.times do |i|
          res = conn.exec_prepared('ext_stmt', [i])
          expect(res[0]['val'].to_i).to eq(i)
        end
      ensure
        pin.exec('ROLLBACK') rescue nil
        pin.close rescue nil
      end
    ensure
      conn.close rescue nil
    end
  end

end
