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

  # Test that the prepared statement that is created inside the connection
  # could not be reliably executed with prepared_statements=false since it
  # could land on another backend connection that doesn't have this
  # statement prepared.
  it 'fails named extended-protocol statements in transaction pool mode' do
    conn = connect
    conn.prepare('ext_stmt', 'SELECT $1::bigint AS val')

    with_pinned_backend do # hold the backend conn prepared on
      with_load do
        20.times do
          expect { conn.exec_prepared('ext_stmt', [1]) }.to raise_error(PG::Error)
        end
      end
    end
  ensure
    conn.close rescue nil
  end

  # Test that a statement created with SQL PREPARE inside the connection
  # could not be reliably executed with prepared_statements=false since the
  # EXECUTE could land on another backend connection that doesn't have this
  # statement prepared.
  it 'fails SQL PREPARE/EXECUTE in transaction pool mode' do
    conn = connect
    # PREPARE and EXECUTE both route to the primary pool (pgdog treats them as
    # writes), the same pool the pin holds a backend in. disabled mode never
    # replays them, so every EXECUTE forced onto another backend fails.
    conn.exec('PREPARE sql_stmt AS SELECT $1::bigint * 2 AS val')

    with_pinned_backend do # hold the backend conn prepared on
      with_load do
        20.times do
          expect { conn.exec('EXECUTE sql_stmt(1)') }.to raise_error(PG::Error)
        end
      end
    end
  ensure
    conn.close rescue nil
  end
end
