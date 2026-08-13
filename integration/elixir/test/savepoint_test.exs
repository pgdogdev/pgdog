defmodule Pgdog.SavepointTest do
  use ExUnit.Case, async: false

  # Regression tests for https://github.com/pgdogdev/pgdog/issues/1314.
  #
  # Postgrex's mode: :savepoint wraps each query in a savepoint by sending
  # Bind, Execute and a simple Query("RELEASE SAVEPOINT postgrex_query") in one batch (no Sync in that batch)
  #
  # When that Execute errors, the server discards everything until a Sync arrives,
  # so the Query's ReadyForQuery is never produced.
  #
  # PgDog used to keep waiting for it and never read the Sync the client had already sent

  setup do
    %{conn: Pgdog.connect()}
  end

  test "query error under mode: :savepoint returns instead of hanging", %{conn: conn} do
    {:ok, {:error, error}} =
      Postgrex.transaction(
        conn,
        fn c -> Postgrex.query(c, "SELECT 1/0", [], mode: :savepoint) end,
        timeout: 5_000
      )

    assert %Postgrex.Error{postgres: %{code: :division_by_zero}} = error
  end

  test "connection survives a savepoint-mode error", %{conn: conn} do
    {:ok, {:error, _}} =
      Postgrex.transaction(
        conn,
        fn c -> Postgrex.query(c, "SELECT 1/0", [], mode: :savepoint) end,
        timeout: 5_000
      )

    assert Pgdog.one(Postgrex.query!(conn, "SELECT 1::bigint", [])) == 1
  end

  test "savepoint-mode query that succeeds still works", %{conn: conn} do
    {:ok, {:ok, result}} =
      Postgrex.transaction(
        conn,
        fn c -> Postgrex.query(c, "SELECT 2::bigint", [], mode: :savepoint) end,
        timeout: 5_000
      )

    assert Pgdog.one(result) == 2
  end
end
