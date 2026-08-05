import Config

# Postgrex bakes the JSON library into its default type module at compile
# time, so this has to be configured here rather than at runtime.
config :postgrex, json_library: Jason

# Postgrex logs a disconnect at :error before the test that caused it gets a
# chance to assert on it. Keep the noise down; failures still surface as
# ExUnit assertions.
config :logger, level: :warning
