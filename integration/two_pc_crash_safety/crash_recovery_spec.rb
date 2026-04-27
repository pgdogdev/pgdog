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

    # Hold shard_1's responses so PREPARE TRANSACTION on shard_1 hangs
    # waiting for the reply (latency, not timeout: latency delays bytes
    # without closing the connection, which is what we need to catch
    # pgdog mid-2PC instead of letting it bail and roll back).
    Toxiproxy[:crash_safety_shard_1].toxic(:latency, latency: 30_000).apply do
      Thread.new do
        begin
          client.exec('COMMIT')
        rescue PG::Error
          # connection drops when we kill pgdog; expected.
        end
      end
      # Give pgdog enough time to issue PREPARE on shard_0 (real
      # backend, fast) and then start blocking on shard_1's response
      # before we kill it.
      sleep 1.0
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

    expect(metric('two_pc_recovered_total')).to be > 0,
      'restarted pgdog reports zero recovered txns; recovery did not run'

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

  it 'no recovery work after a clean shutdown' do
    client = pgdog_conn
    client.exec('BEGIN')
    client.exec("INSERT INTO crash_safety_test (id, value) VALUES (0, 'a')")
    client.exec("INSERT INTO crash_safety_test (id, value) VALUES (1, 'b')")
    client.exec('COMMIT')
    client.close

    # SIGTERM lets Manager::shutdown drain and wal.append_end every
    # finished txn. The WAL on disk should describe a complete cycle
    # (Begin, Committing, End) for every txn so recovery has nothing
    # to do on the next start.
    stop_pgdog(@pid, signal: 'TERM')
    @pid = nil

    @pid, _ = spawn_pgdog(CONFIG_DIR, wal_dir: @wal_dir)
    wait_for_pgdog

    expect(metric('two_pc_recovered_total')).to eq(0),
      'clean-shutdown WAL should have left no in-flight txns to recover'
  end

  it 'second pgdog refuses to share the WAL directory' do
    # First pgdog (started in before) holds the flock on @wal_dir/.lock.
    # Spawning a second one against the same dir should fail to acquire
    # the lock; enable_wal warns and continues without WAL durability,
    # so we verify the failure via the log line.
    log_path = File.join(CONFIG_DIR, 'pgdog-second.log')
    second_pid = Process.spawn(
      {
        'PGDOG_TWO_PHASE_COMMIT_WAL_DIR' => @wal_dir,
        'PGDOG_PORT' => '6433',
      },
      ENV['PGDOG_BIN'] || File.expand_path('../../target/release/pgdog', __dir__),
      '--config', File.join(CONFIG_DIR, 'pgdog.toml'),
      '--users', File.join(CONFIG_DIR, 'users.toml'),
      out: log_path, err: %i[child out]
    )

    begin
      # Give the second pgdog enough time to attempt enable_wal and log
      # the failure.
      sleep 2
      stop_pgdog(second_pid)

      log = File.read(log_path)
      expect(log).to include('locked by another process'),
        "expected DirLocked error in second pgdog log; got:\n#{log}"
    ensure
      FileUtils.rm_f(log_path)
    end
  end
end
