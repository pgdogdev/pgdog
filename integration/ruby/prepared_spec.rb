# frozen_string_literal: true

require_relative 'rspec_helper'

# Uses the main integration pgdog.toml which sets prepared_statements = "extended".
# "extended" rewrites and replays named extended-protocol statements (Parse/Bind)
# across backends, but does NOT intercept SQL PREPARE / EXECUTE.


describe 'prepared_statements = extended' do
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
  it 'passes named statements in session mode' do
    conn = connect('pgdog', 'pgdog_session')
    conn.prepare('session_stmt', 'SELECT $1::bigint AS val')
    10.times do |i|
      res = conn.exec_prepared('session_stmt', [i])
      expect(res[0]['val'].to_i).to eq(i)
    end
    conn.close
  end

  # The rewrite is per-frontend (per client connection). conn2 has no mapping
  # for 'cross_stmt', so its Bind is forwarded with the original name to a
  # backend that never saw the Parse for that name.
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

  # extended mode renames each frontend's Parse to an internal name
  # (__pgdog_N, unique per frontend) and replays it on any backend before
  # the Bind. 5 threads × 20 iterations with a pool of 10 forces genuine
  # crossings; the replay ensures all succeed.
  # Result values are verified to guard against silent data corruption.
  it 'executes named extended-protocol statements in transaction pool mode' do
    errors = []
    mutex  = Mutex.new

    threads = 5.times.map do
      Thread.new do
        conn = connect
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

  # extended mode does NOT intercept SQL PREPARE / EXECUTE — those are
  # forwarded as-is. With 5 threads and a pool of 10, at least some threads
  # will prepare 'sql_stmt' on a backend that already holds it ('already
  # exists') or execute on a backend that has never seen the PREPARE ('does
  # not exist'). Either way, errors accumulate.
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
