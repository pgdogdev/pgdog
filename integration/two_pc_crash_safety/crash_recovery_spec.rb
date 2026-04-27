# frozen_string_literal: true

require_relative 'rspec_helper'

CONFIG_DIR = __dir__

describe '2pc crash safety' do
  let(:wal_dir) { @wal_dir }

  before(:each) do
    cleanup_prepared_xacts!
    @pid, @wal_dir = spawn_pgdog(CONFIG_DIR)
    wait_for_pgdog
  end

  after(:each) do
    stop_pgdog(@pid)
    FileUtils.rm_rf(@wal_dir) if @wal_dir
  end

  it 'rolls back orphan prepared xacts after a kill mid-PREPARE' do
    client = pgdog_conn
    client.exec('BEGIN')
    client.exec("INSERT INTO crash_safety_test (id, value) VALUES (0, 'a')")
    client.exec("INSERT INTO crash_safety_test (id, value) VALUES (1, 'b')")

    # Hold shard_1's responses so PREPARE TRANSACTION on shard_1 hangs.
    # Once pgdog is stuck mid-COMMIT, kill -9 it. The toxic block exits
    # automatically when the spec moves on, but the pgdog process is
    # already dead so there's nothing to unblock.
    Toxiproxy[:crash_safety_shard_1].toxic(:timeout, timeout: 0).apply do
      Thread.new do
        begin
          client.exec('COMMIT')
        rescue PG::Error
          # connection drops when we kill pgdog; expected.
        end
      end
      # Give pgdog enough time to issue PREPARE on shard_0 before we
      # kill it. shard_0 is a real backend so its PREPARE returns fast.
      sleep 0.5
      Process.kill('KILL', @pid)
      Process.waitpid(@pid)
      @pid = nil
    end

    # Sanity check: at least one shard should have an orphan prepared
    # xact left behind. Otherwise the test passes trivially because
    # there was nothing for recovery to clean up.
    leftover = SHARDS.flat_map { |s| prepared_xacts(s) }
    expect(leftover).not_to be_empty,
      'no prepared xacts after kill; the kill landed before any PREPARE made it through'

    # Restart pgdog with the same WAL directory. Recovery sees the
    # Begin record and drives ROLLBACK PREPARED on every participant.
    @pid, _ = spawn_pgdog(CONFIG_DIR, wal_dir: @wal_dir)
    wait_for_pgdog

    expect(wait_for_no_prepared_xacts).to be(true),
      lambda {
        leftover = SHARDS.flat_map { |s| prepared_xacts(s) }
        "prepared xacts did not drain: #{leftover.inspect}"
      }

    SHARDS.each do |shard|
      c = shard_conn(shard)
      count = c.exec('SELECT COUNT(*) FROM crash_safety_test').to_a[0]['count'].to_i
      c.close
      expect(count).to eq(0), "expected no rows on #{shard[:db]}, found #{count}"
    end
  end
end
