# frozen_string_literal: true

require_relative 'rspec_helper'

def connect(dbname = 'pgdog', user = 'pgdog')
  PG.connect(dbname: dbname, user: user, password: 'pgdog', port: 6432, host: '127.0.0.1', application_name: '')
end

describe 'protocol out of sync' do
  after do
    ensure_done
  end

  # In pipeline mode, a failed first query must not prevent subsequent queries
  # from executing. Seq2 and seq3 must return rows even when seq1 errors.
  it 'extended query pipeline: error in seq1 does not drop seq2 and seq3' do
    conn = connect

    conn.enter_pipeline_mode

    # Seq1: will fail — division by zero
    conn.send_query_params 'SELECT 1/0', []
    conn.pipeline_sync
    # Seq2: must succeed
    conn.send_query_params 'SELECT $1::integer AS val', [2]
    conn.pipeline_sync
    # Seq3: must succeed
    conn.send_query_params 'SELECT $1::integer AS val', [3]
    conn.pipeline_sync

    # Seq1: fails — consume the error result, then the sync boundary.
    begin
      conn.get_result
    rescue PG::Error => e
      expect(e.message).to include('division by zero')
    end
    conn.get_result # nil — end of command
    conn.get_result # PipelineSync — sync boundary

    # Seq2: must return a real row — if aborted instead, the request was dropped.
    r2 = conn.get_result
    expect(r2.result_status).to eq(PG::PGRES_TUPLES_OK)
    expect(r2[0]['val']).to eq('2')
    conn.get_result # end of command
    conn.get_result # PipelineSync

    # Seq3: must return a real row.
    r3 = conn.get_result
    expect(r3.result_status).to eq(PG::PGRES_TUPLES_OK)
    expect(r3[0]['val']).to eq('3')
    conn.get_result # end of command
    conn.get_result # PipelineSync

    conn.exit_pipeline_mode
    conn.close
  end
end
