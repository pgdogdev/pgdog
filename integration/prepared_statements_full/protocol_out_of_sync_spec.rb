# frozen_string_literal: true

require_relative 'rspec_helper'

# Triggers the failed-prepare/orphaned-EXECUTE bug (Issue 1).
def trigger_prepare_inject_failure(conn, statement_name:)
  # 1. PREPARE fails — pgdog keeps the statement in its local cache despite the error.
  expect { conn.exec "PREPARE #{statement_name} AS SELECT 1 FROM __pgdog_nonexistent_table__" }
    .to raise_error(PG::Error, /__pgdog_nonexistent_table__/)

  # 2. EXECUTE triggers [Prepare, Query] injection; injected PREPARE fails again; stale EXECUTE E+Z left on wire.
  expect { conn.exec "EXECUTE #{statement_name}" }
    .to raise_error(PG::Error, /__pgdog_nonexistent_table__/)
end

describe 'protocol out of sync regressions' do
  after do
    ensure_done
  end

  # Session mode: orphaned EXECUTE ReadyForQuery hits empty queue (got: Z, extended: false).
  it 'orphaned EXECUTE RFQ hits empty queue in session mode' do
    conn = connect_pgdog(user: 'pgdog_session')
    begin
      # 1. Leave stale EXECUTE E+Z on TCP buffer.
      trigger_prepare_inject_failure(conn, statement_name: 'pgdog_prepare_inject_session')

      # 2. Next query: stale E clears queue; stale Z hits empty queue → ProtocolOutOfSync got: Z.
      expect { conn.exec 'SELECT 1 AS alive' }
        .to raise_error(PG::ConnectionBad, /FATAL:\s*protocol is out of sync/)
    ensure
      conn.close unless conn.finished?
    end
  end

  # Transaction mode (pool_size=1): stale-chain — 'T'-status RFQ keeps server alive; INSERT hits empty queue (got: C).
  it 'stale-chain in transaction mode produces ProtocolOutOfSync got: C' do
    conn = connect_pgdog(user: 'pgdog_tx_single')
    begin
      # 1. Leave stale EXECUTE E+'I'-Z on TCP buffer.
      trigger_prepare_inject_failure(conn, statement_name: 'pgdog_prepare_inject_tx')

      tmp = "#{Process.pid}_#{rand(1_000_000)}"

      # 2. CREATE TABLE — consumes stale E+'I'-Z; client sees InvalidSqlStatementName.
      write_sql = "CREATE TEMP TABLE pgdog_prepare_inject_#{tmp} (id int)"
      expect { conn.exec write_sql }
        .to raise_error(PG::InvalidSqlStatementName, /prepared statement ".*" does not exist/)

      # 3. INSERT (1) — consumes stale C+'I'-Z; client sees stale CREATE TABLE result.
      conn.exec "INSERT INTO pgdog_prepare_inject_#{tmp} (id) VALUES (1)"

      # 4. BEGIN — consumes stale C+'I'-Z; actual C+'T'-Z lands in pool.
      conn.exec 'BEGIN'

      # 5. INSERT (2) — consumes stale C+'T'-Z; 'T' sets in_transaction=true → actual INSERT hits empty queue.
      expect do
        conn.exec "INSERT INTO pgdog_prepare_inject_#{tmp} (id) VALUES (2)"
        sleep 0.05  # let event loop process actual INSERT before Ruby sends END
        conn.exec 'END'
      end.to raise_error(PG::ConnectionBad, /FATAL:\s*protocol is out of sync/)
    ensure
      conn.close unless conn.finished?
    end
  end

  # Session mode with prior exec_params: extended=true set permanently; same got: Z result.
  it 'orphaned EXECUTE RFQ hits empty queue after extended query in session mode' do
    conn = connect_pgdog(user: 'pgdog_session')
    begin
      # 1. exec_params permanently sets extended=true on this connection.
      result = conn.exec_params('SELECT $1::int AS primer', [42])
      expect(result.first['primer']).to eq('42')

      # 2. Leave stale EXECUTE E+Z on TCP buffer.
      trigger_prepare_inject_failure(conn, statement_name: 'pgdog_prepare_inject_ext')

      # 3. Next query: stale E clears queue; stale Z hits empty queue → ProtocolOutOfSync got: Z.
      expect { conn.exec 'SELECT 1 AS alive' }
        .to raise_error(PG::ConnectionBad, /FATAL:\s*protocol is out of sync/)
    ensure
      conn.close unless conn.finished?
    end
  end
end
