ExUnit.start()

# Configuration for connecting to pgdog
defmodule TestConfig do
  def connection_opts do
    [
      hostname: "127.0.0.1",
      port: 6432,
      username: "pgdog",
      password: "pgdog",
      database: "pgdog"
    ]
  end
end