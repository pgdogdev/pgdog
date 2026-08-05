defmodule Pgdog.BasicTest do
  use ExUnit.Case, async: false

  setup do
    %{conn: Pgdog.connect()}
  end

  test "simple select", %{conn: conn} do
    assert Pgdog.one(Postgrex.query!(conn, "SELECT 1::bigint", [])) == 1
  end

  test "scalar parameters round trip", %{conn: conn} do
    cases = [
      {"bigint", 9_223_372_036_854_775_807},
      {"int", -42},
      {"smallint", 5},
      {"text", "hello ✨"},
      {"boolean", true},
      {"float8", 1.5},
      {"bytea", <<0, 1, 254, 255>>},
      {"bigint[]", [1, 2, 3]},
      {"text[]", ["a", "b"]},
      {"text", nil}
    ]

    for {type, value} <- cases do
      result = Postgrex.query!(conn, "SELECT $1::#{type}", [value])
      assert Pgdog.one(result) == value, "#{type} did not round trip"
    end
  end

  test "numeric round trips as a Decimal", %{conn: conn} do
    value = Decimal.new("1234.5678")

    assert Postgrex.query!(conn, "SELECT $1::numeric", [value])
           |> Pgdog.one()
           |> Decimal.equal?(value)
  end

  test "timestamptz round trips", %{conn: conn} do
    value = ~U[2024-03-01 12:34:56Z]
    result = Pgdog.one(Postgrex.query!(conn, "SELECT $1::timestamptz", [value]))
    assert DateTime.compare(result, value) == :eq
  end

  test "uuid round trips as raw bytes", %{conn: conn} do
    value = :crypto.strong_rand_bytes(16)
    assert Pgdog.one(Postgrex.query!(conn, "SELECT $1::uuid", [value])) == value
  end

  test "jsonb round trips", %{conn: conn} do
    value = %{"a" => [1, 2, 3], "b" => nil, "c" => %{"nested" => true}}
    assert Pgdog.one(Postgrex.query!(conn, "SELECT $1::jsonb", [value])) == value
  end

  test "multi-column, multi-row results", %{conn: conn} do
    result =
      Postgrex.query!(
        conn,
        "SELECT i, i * 2 AS doubled FROM generate_series(1, 5) AS i ORDER BY i",
        []
      )

    assert result.columns == ["i", "doubled"]
    assert result.rows == [[1, 2], [2, 4], [3, 6], [4, 8], [5, 10]]
    assert result.num_rows == 5
  end

  test "a query error leaves the connection usable", %{conn: conn} do
    assert {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} =
             Postgrex.query(conn, "SELECT * FROM elixir_no_such_table", [])

    assert Pgdog.one(Postgrex.query!(conn, "SELECT 1::bigint", [])) == 1
  end

  test "transactions commit and roll back", %{conn: conn} do
    Postgrex.query!(conn, "DROP TABLE IF EXISTS elixir_tx", [])
    Postgrex.query!(conn, "CREATE TABLE elixir_tx (id BIGINT PRIMARY KEY)", [])

    assert {:ok, :committed} =
             Postgrex.transaction(conn, fn tx ->
               Postgrex.query!(tx, "INSERT INTO elixir_tx (id) VALUES ($1)", [1])
               :committed
             end)

    assert {:error, :nope} =
             Postgrex.transaction(conn, fn tx ->
               Postgrex.query!(tx, "INSERT INTO elixir_tx (id) VALUES ($1)", [2])
               Postgrex.rollback(tx, :nope)
             end)

    assert Pgdog.column(Postgrex.query!(conn, "SELECT id FROM elixir_tx ORDER BY id", [])) == [1]
  end

  test "writes are visible inside the transaction that made them", %{conn: conn} do
    Postgrex.query!(conn, "DROP TABLE IF EXISTS elixir_read_own_writes", [])
    Postgrex.query!(conn, "CREATE TABLE elixir_read_own_writes (id BIGINT PRIMARY KEY)", [])

    Postgrex.transaction(conn, fn tx ->
      Postgrex.query!(tx, "INSERT INTO elixir_read_own_writes (id) VALUES ($1)", [7])

      assert Pgdog.column(Postgrex.query!(tx, "SELECT id FROM elixir_read_own_writes", [])) == [7]
    end)
  end

  test "concurrent queries over a pool" do
    conn = Pgdog.connect(pool_size: 10)

    results =
      1..50
      |> Task.async_stream(
        fn i -> Pgdog.one(Postgrex.query!(conn, "SELECT $1::bigint", [i])) end,
        max_concurrency: 10,
        ordered: true
      )
      |> Enum.map(fn {:ok, value} -> value end)

    assert results == Enum.to_list(1..50)
  end

  test "COPY ... TO STDOUT streams rows", %{conn: conn} do
    result = Postgrex.query!(conn, "COPY (SELECT generate_series(1, 3)) TO STDOUT", [])
    assert IO.iodata_to_binary(result.rows) == "1\n2\n3\n"
  end

  # COPY FROM STDIN over the extended protocol is accepted and reported as a
  # success, but no rows arrive. `psql` (simple protocol) through the same
  # PgDog loads the rows, and this same Postgrex code against PostgreSQL
  # directly loads them too, so PgDog is dropping the CopyData payload.
  @tag :known_bug
  test "COPY ... FROM STDIN loads rows", %{conn: conn} do
    Postgrex.query!(conn, "DROP TABLE IF EXISTS elixir_copy", [])
    Postgrex.query!(conn, "CREATE TABLE elixir_copy (id BIGINT, value TEXT)", [])

    Postgrex.transaction(conn, fn tx ->
      stream = Postgrex.stream(tx, "COPY elixir_copy FROM STDIN", [])
      Enum.into(["1\tone\n", "2\ttwo\n"], stream)
    end)

    assert Postgrex.query!(conn, "SELECT id, value FROM elixir_copy ORDER BY id", []).rows ==
             [[1, "one"], [2, "two"]]
  end
end
