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
    expires_in: 3600,
    email: 'alice@example.com',
    verified_email: true
  },
  'expired-google-token' => {
    audience: 'gcloud-client',
    user_id: '1234567890',
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    expires_in: 0,
    email: 'alice@example.com',
    verified_email: true
  },
  'wrong-email-token' => {
    audience: 'gcloud-client',
    user_id: '9876543210',
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    expires_in: 3600,
    email: 'bob@example.com',
    verified_email: true
  },
  'missing-scope-token' => {
    audience: 'gcloud-client',
    user_id: '1234567890',
    scope: 'openid email',
    expires_in: 3600,
    email: 'alice@example.com',
    verified_email: true
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
end
