# frozen_string_literal: true

require 'fileutils'
require 'pg'
require 'tmpdir'
require 'toxiproxy'

PGDOG_HOST = '127.0.0.1'
PGDOG_PORT = 6432
SHARDS = [
  { db: 'shard_0', port: 5432 },
  { db: 'shard_1', port: 5432 } # we always look at the *real* port for state checks
].freeze

def pgdog_conn(database: 'pgdog')
  PG.connect(host: PGDOG_HOST, port: PGDOG_PORT, user: 'pgdog',
             password: 'pgdog', dbname: database)
end

def shard_conn(shard)
  PG.connect(host: '127.0.0.1', port: shard[:port], user: 'pgdog',
             password: 'pgdog', dbname: shard[:db])
end

def prepared_xacts(shard)
  c = shard_conn(shard)
  rows = c.exec('SELECT gid FROM pg_prepared_xacts').to_a.map { |r| r['gid'] }
  c.close
  rows
end

def cleanup_prepared_xacts!
  SHARDS.each do |shard|
    c = shard_conn(shard)
    c.exec('SELECT gid FROM pg_prepared_xacts').to_a.each do |row|
      gid = c.escape_string(row['gid'])
      c.exec("ROLLBACK PREPARED '#{gid}'")
    end
    c.exec('TRUNCATE crash_safety_test')
    c.close
  end
end

# Spawns pgdog as a child process with our test config. If `wal_dir`
# is given, the spawned pgdog uses it (so a restart resumes the same
# durable log); otherwise a fresh tmpdir is created. Returns
# [pid, wal_dir].
def spawn_pgdog(config_dir, wal_dir: nil)
  wal_dir ||= Dir.mktmpdir('pgdog_wal_')
  binary = ENV['PGDOG_BIN'] || File.expand_path('../../target/release/pgdog', __dir__)
  unless File.exist?(binary)
    system('cargo', 'build', '--release', chdir: File.expand_path('../..', __dir__)) ||
      raise('cargo build failed')
  end
  log_path = File.join(config_dir, 'pgdog.log')
  pid = Process.spawn(
    { 'PGDOG_TWO_PHASE_COMMIT_WAL_DIR' => wal_dir },
    binary,
    '--config', File.join(config_dir, 'pgdog.toml'),
    '--users', File.join(config_dir, 'users.toml'),
    out: log_path, err: [:child, :out]
  )
  [pid, wal_dir]
end

def wait_for_pgdog(timeout: 10)
  deadline = Time.now + timeout
  loop do
    begin
      c = pgdog_conn
      c.exec('SELECT 1')
      c.close
      return
    rescue PG::Error
      raise "pgdog did not become ready within #{timeout}s" if Time.now > deadline

      sleep 0.1
    end
  end
end

def stop_pgdog(pid, signal: 'TERM', timeout: 10)
  return unless pid

  begin
    Process.kill(signal, pid)
  rescue Errno::ESRCH
    return
  end
  deadline = Time.now + timeout
  loop do
    _, status = Process.waitpid2(pid, Process::WNOHANG)
    return if status

    if Time.now > deadline
      Process.kill('KILL', pid) rescue nil
      Process.waitpid(pid) rescue nil
      return
    end
    sleep 0.05
  end
end

# Wait for both shards' pg_prepared_xacts to drain. Returns true if it
# happened within the deadline, false otherwise.
def wait_for_no_prepared_xacts(timeout: 30)
  deadline = Time.now + timeout
  loop do
    return true if SHARDS.all? { |s| prepared_xacts(s).empty? }
    return false if Time.now > deadline

    sleep 0.2
  end
end

Toxiproxy.host = 'http://127.0.0.1:8474'
