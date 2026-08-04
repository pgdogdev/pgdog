defmodule Pgdog do
  @moduledoc """
  Connection helpers for the Elixir integration suite.

  Everything here connects through PgDog on 127.0.0.1:6432, never straight to
  PostgreSQL, so all traffic is proxied.
  """

  @host "127.0.0.1"
  @port 6432

  @doc """
  Start a Postgrex connection linked to the calling process.

  `opts` are merged over the defaults, so `connect(database: "pgdog_sharded")`
  points at the sharded pool.
  """
  @spec connect(keyword()) :: pid()
  def connect(opts \\ []) do
    defaults = [
      hostname: @host,
      port: @port,
      username: "pgdog",
      password: "pgdog",
      database: "pgdog",
      pool_size: 1,
      # Surface connection problems as test failures instead of quietly
      # reconnecting until the test times out.
      backoff_type: :stop
    ]

    {:ok, conn} = Postgrex.start_link(Keyword.merge(defaults, opts))
    conn
  end

  @doc "The single value of a one-row, one-column result."
  @spec one(Postgrex.Result.t()) :: term()
  def one(%Postgrex.Result{rows: [[value]]}), do: value

  @doc "The values of a single-column result, unwrapped."
  @spec column(Postgrex.Result.t()) :: [term()]
  def column(%Postgrex.Result{rows: rows}), do: Enum.map(rows, fn [value] -> value end)
end
