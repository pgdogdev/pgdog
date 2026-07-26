# frozen_string_literal: true

require 'pg'
require 'rspec'

# PgDog's stdout/stderr is redirected here by integration/common.sh's run_pgdog.
# The authentication driver warn!-logs deny reasons, so the suite can assert
# they land in the log but never reach the client.
LOG_FILE = File.expand_path('../../../log.txt', __FILE__)

GENERIC_AUTH_ERROR = /is wrong, or the database does not exist/

def connect(user, password, dbname: 'pgdog')
  # Hash form avoids URL-encoding the ':' in credentials like
  # "impersonate:reporting".
  PG.connect(host: '127.0.0.1', port: 6432, user: user, password: password, dbname: dbname)
end

def current_user(conn)
  conn.exec('SELECT current_user AS u')[0]['u']
end

# Poll the PgDog log for a line matching `pattern`. Rust's stdout is
# line-buffered even when redirected, so a short poll is enough.
def wait_for_log(pattern, timeout: 5.0)
  deadline = Time.now + timeout
  loop do
    return true if File.exist?(LOG_FILE) && File.read(LOG_FILE).match?(pattern)
    return false if Time.now > deadline

    sleep 0.1
  end
end

describe 'authentication plugin' do
  it 'allows a client whose credential matches secret-<user> and can query' do
    conn = connect('alice', 'secret-alice')
    expect(conn.exec('SELECT 1 AS n')[0]['n'].to_i).to eq(1)
    conn.close
  end

  it 'rejects a wrong credential with a generic auth error' do
    expect { connect('alice', 'secret-bob') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)
  end

  it 'denies "deny" generically, logging the reason only in PgDog' do
    error = nil
    begin
      connect('alice', 'deny')
      raise 'expected the connection to be denied'
    rescue PG::ConnectionBad => e
      error = e
    end

    # Client sees the generic error, never the plugin's reason.
    expect(error.message).to match(GENERIC_AUTH_ERROR)
    expect(error.message).not_to include('test deny')

    # PgDog logs the actual reason for the operator.
    expect(wait_for_log(/denied by plugin .*test deny/)).to be(true)
  end

  it 'survives a panicking plugin and keeps serving' do
    expect { connect('alice', 'panic') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)

    # PgDog is still alive: a subsequent good login works.
    conn = connect('alice', 'secret-alice')
    expect(conn.exec('SELECT 1 AS n')[0]['n'].to_i).to eq(1)
    conn.close
  end

  it 'denies an unknown credential when every plugin skips' do
    expect { connect('alice', 'no-such-credential') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)
  end

  it 'provisions a pool and impersonates the derived role' do
    conn = connect('reporting', 'impersonate:reporting')
    expect(current_user(conn)).to eq('reporting')
    conn.close
  end

  it 'keeps the impersonated role across connection reuse and cleanup' do
    conn = connect('reporting', 'impersonate:reporting')

    # Several transactions force the backend connection through the pool's
    # cleanup path (RESET ALL / DISCARD ALL) between checkouts. Dirtying the
    # session with a SET must not clear the role, which is a startup-parameter
    # session default rather than a runtime SET.
    10.times do
      conn.exec('BEGIN')
      conn.exec("SET work_mem = '8MB'")
      expect(current_user(conn)).to eq('reporting')
      conn.exec('COMMIT')
    end

    expect(current_user(conn)).to eq('reporting')
    conn.close
  end

  it 'rejects role escapes on the impersonation pool but keeps the session usable' do
    conn = connect('reporting', 'impersonate:reporting')

    escapes = [
      'SET ROLE someone_else',
      'RESET ROLE',
      'SET SESSION AUTHORIZATION someone_else',
      # Multi-statement batch: the role change must be rejected even when it
      # rides along with an innocuous statement.
      'SELECT 1; SET ROLE someone_else',
      # set_config with a non-constant value would otherwise pass through
      # verbatim, escaping the guard.
      "SELECT set_config('role', 'some' || 'one_else', false)"
    ]
    escapes.each do |stmt|
      expect { conn.exec(stmt) }.to raise_error(PG::Error, /impersonates a fixed role/)
      # Session survives the rejection and the role is unchanged.
      expect(current_user(conn)).to eq('reporting')
    end

    # A multi-statement batch with no role change is still accepted.
    conn.exec('SELECT 1; SELECT 2')
    expect(current_user(conn)).to eq('reporting')

    conn.close
  end

  it 'rejects INSERT on a read-only pool' do
    conn = connect('readonly', 'secret-readonly')
    expect { conn.exec('INSERT INTO auth_test (id) VALUES (1)') }
      .to raise_error(PG::Error, /read-only transaction/)
    conn.close
  end
end
