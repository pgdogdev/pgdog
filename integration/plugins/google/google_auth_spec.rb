# frozen_string_literal: true

require 'pg'
require 'rspec'
require 'json'
require 'socket'
require 'uri'

GENERIC_AUTH_ERROR = /is wrong, or the database does not exist/

TOKEN_RESPONSES = {
  'valid-google-token' => {
    audience: 'gcloud-client',
    user_id: '1234567890',
    scope: 'openid email https://www.googleapis.com/auth/cloud-platform',
    expires_in: '3600',
    email: 'alice@example.com',
    verified_email: 'true'
  },
  'expired-google-token' => {
    audience: 'gcloud-client',
    user_id: '1234567890',
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    expires_in: '0',
    email: 'alice@example.com',
    verified_email: 'true'
  },
  'wrong-email-token' => {
    audience: 'gcloud-client',
    user_id: '9876543210',
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    expires_in: '3600',
    email: 'bob@example.com',
    verified_email: 'true'
  },
  'missing-scope-token' => {
    audience: 'gcloud-client',
    user_id: '1234567890',
    scope: 'openid email',
    expires_in: '3600',
    email: 'alice@example.com',
    verified_email: 'true'
  },
  'dave-google-token' => {
    audience: 'gcloud-client',
    user_id: '2222222222',
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    expires_in: '3600',
    email: 'dave@example.com',
    verified_email: 'true'
  },
  'carol-google-token' => {
    audience: 'gcloud-client',
    user_id: '3333333333',
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    expires_in: '3600',
    email: 'carol@example.com',
    verified_email: 'true'
  }
}.freeze

class TokenInfoServer
  def initialize
    @server = TCPServer.new('127.0.0.1', 18_080)
    @thread = Thread.new { serve }
  end

  def stop
    @server.close
    @thread.join
  end

  private

  def serve
    loop do
      socket = @server.accept
      request_line = socket.gets
      while (line = socket.gets)
        break if line == "\r\n"
      end

      token = request_line && access_token(request_line)
      body = TOKEN_RESPONSES[token]
      if body
        respond(socket, '200 OK', JSON.generate(body))
      else
        respond(socket, '400 Bad Request', JSON.generate(error: 'invalid_token'))
      end
    rescue IOError, Errno::EBADF
      break
    ensure
      socket&.close
    end
  end

  def access_token(request_line)
    target = request_line.split[1]
    URI.decode_www_form(URI(target).query.to_s).to_h['access_token']
  end

  def respond(socket, status, body)
    socket.write(
      "HTTP/1.1 #{status}\r\n" \
      "Content-Type: application/json\r\n" \
      "Content-Length: #{body.bytesize}\r\n" \
      "Connection: close\r\n\r\n" \
      "#{body}"
    )
  end
end

def connect(user, token)
  PG.connect(
    host: '127.0.0.1',
    port: 6432,
    user: user,
    password: token,
    dbname: 'pgdog'
  )
end

describe 'Google access-token authentication plugin' do
  before(:all) do
    @token_info = TokenInfoServer.new
  end

  after(:all) do
    @token_info.stop
  end

  it 'accepts a valid Google access token and runs a query' do
    conn = connect('alice@example.com', 'valid-google-token')
    expect(conn.exec('SELECT 1 AS n')[0]['n'].to_i).to eq(1)
    conn.close
  end

  it 'impersonates the Google identity on a pre-configured pool' do
    # alice's users.toml entry has no `server_role`; the plugin's grant fills
    # it, so queries run as the authenticated identity, not the service account.
    conn = connect('alice@example.com', 'valid-google-token')
    expect(conn.exec('SELECT current_user AS u')[0]['u']).to eq('alice@example.com')
    conn.close
  end

  it 'skips excluded users so PostgreSQL passthrough can authenticate them' do
    conn = connect('pgdog', 'pgdog')
    expect(conn.exec('SELECT 1 AS n')[0]['n'].to_i).to eq(1)
    conn.close
  end

  it 'rejects a token Google does not recognize with a generic error' do
    expect { connect('alice@example.com', 'invalid-google-token') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)
  end

  it 'rejects an expired token' do
    expect { connect('alice@example.com', 'expired-google-token') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)
  end

  it 'rejects a token for a different Google identity' do
    expect { connect('alice@example.com', 'wrong-email-token') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)
  end

  it 'rejects a token missing a required scope' do
    expect { connect('alice@example.com', 'missing-scope-token') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)
  end

  it 'rejects a valid login for an identity with no pool when provisioning is off' do
    expect { connect('carol@example.com', 'carol-google-token') }
      .to raise_error(PG::ConnectionBad, GENERIC_AUTH_ERROR)
  end

  # Keep this last: it leaves dave's pool unable to connect to Postgres.
  it 'fails the connection when the impersonated role does not exist in Postgres' do
    # dave authenticates and has a configured pool, but setup.sql never created
    # his Postgres role: the backend refuses the `role` startup parameter, so
    # the login (or, with cached server parameters, the first query) fails
    # rather than falling back to the service account.
    expect do
      conn = connect('dave@example.com', 'dave-google-token')
      begin
        conn.exec('SELECT current_user')
      ensure
        conn.close
      end
    end.to raise_error(PG::Error)
  end
end
