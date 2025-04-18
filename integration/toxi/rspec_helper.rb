# frozen_string_literal: true

require 'toxiproxy'
require 'pg'
require 'concurrent'

def conn
  PG.connect 'postgres://pgdog:pgdog@127.0.0.1:6432/failover'
end

def admin
  PG.connect 'postgres://admin:pgdog@127.0.0.1:6432/admin'
end
