# frozen_string_literal: true

require_relative 'rspec_helper'

describe 'prepared_statements = full' do
  after { ensure_done }

  # Mirror of disabled suite: anonymous statements carry no per-backend state,
  # so they work identically regardless of the prepared_statements setting.
  it 'executes anonymous parameterized queries' do
    conn = connect_pgdog
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
    conn = connect_pgdog(user: 'pgdog_session')
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
        conn = connect_pgdog
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
    conn1 = connect_pgdog(user: 'pgdog_session')
    conn2 = connect_pgdog(user: 'pgdog_session')
    conn1.prepare('cross_stmt', 'SELECT $1::bigint AS val')
    expect do
      conn2.exec_prepared('cross_stmt', [7])
    end.to raise_error(PG::Error)
    conn1.close
    conn2.close
  end

  # Mirror of disabled/'fails named extended-protocol statements in
  # transaction pool mode'. full mode renames each frontend's Parse to an
  # internal name (__pgdog_N, unique per frontend) and replays it on any
  # backend before the Bind. 5 threads × 20 iterations with pool_size = 2
  # forces genuine crossings \ the replay ensures all succeed.
  # Result values are also verified to guard against silent data corruption.
  it 'executes named extended-protocol statements in transaction pool mode' do
    errors = []
    mutex  = Mutex.new

    threads = 5.times.map do
      Thread.new do
        conn = connect_pgdog
        begin
          conn.prepare('ext_stmt', 'SELECT $1::bigint AS val')
          20.times do |i|
            res = conn.exec_prepared('ext_stmt', [i])
            val = res[0]['val'].to_i
            raise "expected #{i}, got #{val}" unless val == i
          end
        rescue => e
          mutex.synchronize { errors << e.message }
        ensure
          conn.close rescue nil
        end
      end
    end

    threads.each(&:join)
    expect(errors).to be_empty
  end

end
