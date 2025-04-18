require 'toxiproxy'
require 'pg'

def conn
  return PG.connect "postgres://pgdog:pgdog@127.0.0.1:6432/failover"
end

def admin
  return PG.connect "postgres://admin:pgdog@127.0.0.1:6432/admin"
end
