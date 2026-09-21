defmodule Pgdog.PreparedTest do
  use ExUnit.Case, async: false

  test "SQL PREPARE and EXECUTE preserve the extended protocol" do
    for options <- [[], [prepare: :unnamed]] do
      conn = Pgdog.connect(options)
      name = "elixir_sql_#{System.unique_integer([:positive])}"

      try do
        prepare = Postgrex.prepare!(conn, "prepare_command", "PREPARE #{name} AS SELECT $1::bigint * 2")
        assert %Postgrex.Result{command: :prepare} = Postgrex.execute!(conn, prepare, [])
        execute = Postgrex.prepare!(conn, "execute_command", "EXECUTE #{name}(21)")

        for _ <- 1..3 do
          assert Pgdog.one(Postgrex.execute!(conn, execute, [])) == 42
        end

        assert Pgdog.one(Postgrex.query!(conn, "SELECT 1", [])) == 1
      after
        GenServer.stop(conn)
      end
    end
  end

  test "a named statement is reusable across many executions" do
    conn = Pgdog.connect()
    query = Postgrex.prepare!(conn, "elixir_echo", "SELECT $1::bigint")

    for i <- 1..50 do
      assert Pgdog.one(Postgrex.execute!(conn, query, [i])) == i
    end
  end

  # Postgrex closes statements it no longer needs, which PgDog has to
  # acknowledge with CloseComplete. See also Pgdog.SetTest.
  test "closing a statement frees the name for a different statement" do
    conn = Pgdog.connect()

    query = Postgrex.prepare!(conn, "elixir_close", "SELECT $1::bigint")
    assert Pgdog.one(Postgrex.execute!(conn, query, [1])) == 1
    assert :ok = Postgrex.close(conn, query)

    query = Postgrex.prepare!(conn, "elixir_close", "SELECT $1::text")
    assert Pgdog.one(Postgrex.execute!(conn, query, ["two"])) == "two"
  end

  test "unnamed statements work" do
    conn = Pgdog.connect(prepare: :unnamed)

    for i <- 1..20 do
      assert Pgdog.one(Postgrex.query!(conn, "SELECT $1::bigint", [i])) == i
    end
  end

  test "prepared statements work inside a transaction" do
    conn = Pgdog.connect()
    query = Postgrex.prepare!(conn, "elixir_tx_prepared", "SELECT $1::bigint * 3")

    Postgrex.transaction(conn, fn tx ->
      for i <- 1..10 do
        assert Pgdog.one(Postgrex.execute!(tx, query, [i])) == i * 3
      end
    end)
  end

  # integration/pgdog.toml sets prepared_statements_limit = 500, so preparing
  # more than that forces PgDog to evict entries from its global cache while
  # the client still holds handles to them.
  test "more distinct statements than PgDog's prepared statement cache" do
    conn = Pgdog.connect()

    queries =
      for i <- 1..600 do
        {i, Postgrex.prepare!(conn, "elixir_evict_#{i}", "SELECT $1::bigint + #{i}")}
      end

    for {i, query} <- Enum.take_every(queries, 37) do
      assert Pgdog.one(Postgrex.execute!(conn, query, [0])) == i
    end
  end

  test "the same statement text prepared on many connections" do
    conns = for _ <- 1..8, do: Pgdog.connect()

    conns
    |> Task.async_stream(
      fn conn ->
        query = Postgrex.prepare!(conn, "elixir_shared", "SELECT $1::bigint")
        for i <- 1..20, do: assert(Pgdog.one(Postgrex.execute!(conn, query, [i])) == i)
      end,
      max_concurrency: 8
    )
    |> Stream.run()
  end

  # Same shape as pgdogdev/pgdog#1066, in a spot the fix for that issue didn't
  # cover. `QueryEngine::deallocate` (pgdog/src/frontend/client/query_engine/
  # deallocate.rs) always replies with the simple-protocol pair
  # CommandComplete + ReadyForQuery. A client using the extended protocol is
  # waiting on ParseComplete/ParameterDescription first, so Postgrex gets a
  # CommandComplete it has no clause for and the connection dies.
  # `psql` (simple protocol) is unaffected.
  @tag :known_bug
  test "DEALLOCATE is ignored" do
    conn = Pgdog.connect()
    query = Postgrex.prepare!(conn, "elixir_deallocate", "SELECT $1::bigint")

    assert Pgdog.one(Postgrex.execute!(conn, query, [1])) == 1
    Postgrex.query!(conn, "DEALLOCATE elixir_deallocate", [])
    assert Pgdog.one(Postgrex.execute!(conn, query, [2])) == 2
  end
end
