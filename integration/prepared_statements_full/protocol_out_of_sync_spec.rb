# frozen_string_literal: true

require_relative 'rspec_helper'

# Triggers the failed-prepare / orphaned-EXECUTE scenario.
#
# When pgdog rewrites a simple-query EXECUTE it injects [Prepare, Query].
# If the injected PREPARE fails, the outer EXECUTE sub-request (Error +
# ReadyForQuery) must be consumed internally — nothing stale left on the wire.
def trigger_prepare_inject_failure(conn, statement_name:)
  # PREPARE fails — pgdog caches the statement name despite the error.
  expect { conn.exec "PREPARE #{statement_name} AS SELECT 1 FROM __pgdog_nonexistent_table__" }
    .to raise_error(PG::Error, /__pgdog_nonexistent_table__/)

  # EXECUTE triggers [Prepare, Query] injection; the re-injected PREPARE fails
  # again. pgdog must drain the orphaned EXECUTE E+Z internally and surface
  # only the application-visible error to the caller.
  expect { conn.exec "EXECUTE #{statement_name}" }
    .to raise_error(PG::Error, /__pgdog_nonexistent_table__/)
end

describe 'protocol out of sync regressions' do
  after do
    ensure_done
  end

  # Session mode: a failed prepare-inject must not leave stale bytes on the
  # wire. The connection stays usable for the next query.
  it 'connection remains usable after failed prepare-inject in session mode' do
    conn = connect_pgdog(user: 'pgdog_session')
    begin
      trigger_prepare_inject_failure(conn, statement_name: 'pgdog_prepare_inject_session')

      result = conn.exec 'SELECT 1 AS alive'
      expect(result.first['alive']).to eq('1')
    ensure
      conn.close unless conn.finished?
    end
  end

  # Transaction mode (pool_size=1): the backend connection is returned to the
  # pool after each query. With a single backend any stale bytes not drained
  # internally are visible to the very next borrower. The connection must be
  # clean so the next query succeeds.
  it 'connection remains usable after failed prepare-inject in transaction mode' do
    conn = connect_pgdog(user: 'pgdog_tx_single')
    begin
      trigger_prepare_inject_failure(conn, statement_name: 'pgdog_prepare_inject_tx')

      result = conn.exec 'SELECT 1 AS alive'
      expect(result.first['alive']).to eq('1')
    ensure
      conn.close unless conn.finished?
    end
  end
end
