# frozen_string_literal: true

require_relative 'rspec_helper'

# Triggers the failed-prepare/orphaned-EXECUTE bug (Issue 1).
def trigger_prepare_inject_failure(conn, statement_name:)
  # 1. PREPARE fails — pgdog keeps the statement in its local cache despite the error.
  expect { conn.exec "PREPARE #{statement_name} AS SELECT 1 FROM __pgdog_nonexistent_table__" }
    .to raise_error(PG::Error, /__pgdog_nonexistent_table__/)

  # 2. EXECUTE triggers [Prepare, Query] injection; injected PREPARE fails again.
  #    After fix: pgdog consumes the orphaned EXECUTE E+Z internally; nothing stale on wire.
  expect { conn.exec "EXECUTE #{statement_name}" }
    .to raise_error(PG::Error, /__pgdog_nonexistent_table__/)
end

describe 'protocol out of sync regressions' do
  after do
    ensure_done
  end

  # Issue 1 — Session mode: orphaned EXECUTE ReadyForQuery must not leak to the next query.
  # Bug: stale E+Z left on wire; next query consumed stale E, orphaned Z hit empty queue → ConnectionBad.
  # Fix: Error handler must preserve Code(ReadyForQuery) for the outer EXECUTE when an injected
  #      PREPARE fails; no stale bytes reach the client.
  it 'orphaned EXECUTE RFQ hits empty queue in session mode' do
    conn = connect_pgdog(user: 'pgdog_session')
    begin
      trigger_prepare_inject_failure(conn, statement_name: 'pgdog_prepare_inject_session')

      # After fix: no stale messages on wire; next query must succeed without ConnectionBad.
      result = conn.exec 'SELECT 1 AS alive'
      expect(result.first['alive']).to eq('1')
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

      # 2. CREATE TABLE — must succeed; no stale E+'I'-Z in buffer after fix.
      write_sql = "CREATE TEMP TABLE pgdog_prepare_inject_#{tmp} (id int)"
      conn.exec write_sql

      # 3. INSERT (1) — must succeed with its own real response.
      conn.exec "INSERT INTO pgdog_prepare_inject_#{tmp} (id) VALUES (1)"

      # 4. BEGIN — must succeed; real C+'T'-Z consumed by this query.
      conn.exec 'BEGIN'

      # 5. INSERT (2) + END — must succeed; no stale 'T'-Z in pool to shift the chain.
      conn.exec "INSERT INTO pgdog_prepare_inject_#{tmp} (id) VALUES (2)"
      sleep 0.05  # let event loop process actual INSERT before Ruby sends END
      conn.exec 'END'
    ensure
      conn.close unless conn.finished?
    end
  end

  # Issue 1 — Session mode with prior exec_params: extended=true set permanently.
  # Bug: same as Test 1; extended=true additionally sets out_of_sync=true in the Error handler,
  #      changing connection-lifecycle behaviour. Either way, the next query must not fail.
  # Fix: same root fix; extended flag behaviour (Issue 4) is a separate concern.
  it 'orphaned EXECUTE RFQ hits empty queue after extended query in session mode' do
    conn = connect_pgdog(user: 'pgdog_session')
    begin
      # Parameterised query runs first — sets extended=true on the connection.
      result = conn.exec_params('SELECT $1::int AS primer', [42])
      expect(result.first['primer']).to eq('42')

      trigger_prepare_inject_failure(conn, statement_name: 'pgdog_prepare_inject_ext')

      # After fix: stale E+Z handled internally even with extended=true; next query succeeds.
      result = conn.exec 'SELECT 1 AS alive'
      expect(result.first['alive']).to eq('1')
    ensure
      conn.close unless conn.finished?
    end
  end
end
