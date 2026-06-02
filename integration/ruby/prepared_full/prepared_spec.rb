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

  # Test that a statement created with SQL PREPARE inside the connection
  # can be reliably executed with prepared_statements=full even when the
  # EXECUTE lands on another backend connection, because pgdog replays the
  # PREPARE onto it.
  it 'rewrites simple-protocol PREPARE / EXECUTE in transaction pool mode' do
    conn = connect
    # PREPARE and EXECUTE both route to the primary pool (pgdog treats them as
    # writes), the same pool the pin holds a backend in, so each EXECUTE is forced
    # onto a backend that never saw the PREPARE -- full mode replays it there, so
    # they still succeed.
    conn.exec('PREPARE sql_stmt AS SELECT $1::bigint * 2 AS val')

    with_pinned_backend do # hold the backend conn prepared on
      with_load do
        20.times do |i|
          res = conn.exec("EXECUTE sql_stmt(#{i})")
          expect(res[0]['val'].to_i).to eq(i * 2)
        end
      end
    end
  ensure
    conn.close rescue nil
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
  # can be reliably executed with prepared_statements=full even when it lands
  # on another backend connection, because pgdog replays the prepare onto it.
  it 'executes named extended-protocol statements in transaction pool mode' do
    conn = connect
    conn.prepare('ext_stmt', 'SELECT $1::bigint AS val')

    with_pinned_backend do # hold the backend conn prepared on
      with_load do
        20.times do |i|
          res = conn.exec_prepared('ext_stmt', [i])
          expect(res[0]['val'].to_i).to eq(i)
        end
      end
    end
  ensure
    conn.close rescue nil
  end

end
