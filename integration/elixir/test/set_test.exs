defmodule Pgdog.SetTest do
  use ExUnit.Case, async: false

  # Regression coverage for pgdogdev/pgdog#1066.
  #
  # PgDog answers a SET itself when no backend is checked out yet, faking the
  # responses the client expects. Postgrex uses the extended protocol and
  # closes the statement it just prepared, so that fake response has to include
  # CloseComplete. Without it Postgrex sees a ReadyForQuery where it expected a
  # CloseComplete and tears the connection down.

  setup do
    %{conn: Pgdog.connect()}
  end

  test "SET as the first statement of a transaction", %{conn: conn} do
    assert %Postgrex.Result{} =
             Postgrex.query!(conn, "SET application_name TO 'elixir_set'", [])

    assert Pgdog.one(Postgrex.query!(conn, "SHOW application_name", [])) == "elixir_set"
  end

  test "SET as the first statement, unnamed statements" do
    conn = Pgdog.connect(prepare: :unnamed)

    assert %Postgrex.Result{} =
             Postgrex.query!(conn, "SET application_name TO 'elixir_set_unnamed'", [])

    assert Pgdog.one(Postgrex.query!(conn, "SHOW application_name", [])) == "elixir_set_unnamed"
  end

  test "SET issued from after_connect" do
    conn =
      Pgdog.connect(
        after_connect: {Postgrex, :query!, ["SET application_name TO 'elixir_after_connect'", []]}
      )

    assert Pgdog.one(Postgrex.query!(conn, "SHOW application_name", [])) ==
             "elixir_after_connect"
  end

  test "a custom GUC set before any backend is checked out", %{conn: conn} do
    assert %Postgrex.Result{} = Postgrex.query!(conn, "SET pgdog.role TO 'primary'", [])
    assert Pgdog.one(Postgrex.query!(conn, "SELECT 1::bigint", [])) == 1
  end

  test "SET persists for the life of the session", %{conn: conn} do
    Postgrex.query!(conn, "SET statement_timeout TO '7531ms'", [])

    for _ <- 1..5 do
      assert Pgdog.one(Postgrex.query!(conn, "SHOW statement_timeout", [])) == "7531ms"
    end

    assert Pgdog.one(Postgrex.query!(conn, "SELECT current_setting($1)", ["statement_timeout"])) ==
             "7531ms"
  end

  test "RESET clears the parameter", %{conn: conn} do
    Postgrex.query!(conn, "SET statement_timeout TO '7531ms'", [])
    assert Pgdog.one(Postgrex.query!(conn, "SHOW statement_timeout", [])) == "7531ms"

    Postgrex.query!(conn, "RESET statement_timeout", [])
    refute Pgdog.one(Postgrex.query!(conn, "SHOW statement_timeout", [])) == "7531ms"
  end

  test "SET LOCAL applies only inside its transaction", %{conn: conn} do
    Postgrex.transaction(conn, fn tx ->
      Postgrex.query!(tx, "SET LOCAL statement_timeout TO '9137ms'", [])
      assert Pgdog.one(Postgrex.query!(tx, "SHOW statement_timeout", [])) == "9137ms"
    end)

    refute Pgdog.one(Postgrex.query!(conn, "SHOW statement_timeout", [])) == "9137ms"
  end

  test "set_config returns the value it applied", %{conn: conn} do
    result = Postgrex.query!(conn, "SELECT set_config('statement_timeout', '3577ms', false)", [])

    assert Pgdog.one(result) == "3577ms"
    assert Pgdog.one(Postgrex.query!(conn, "SHOW statement_timeout", [])) == "3577ms"
  end

  test "each session keeps its own parameters" do
    tasks =
      for i <- 0..7 do
        Task.async(fn ->
          conn = Pgdog.connect()
          # Values not divisible by 1000, so Postgres doesn't normalize
          # "1000ms" into "1s" and make the assertion pass for the wrong reason.
          timeout = "#{1001 + i * 113}ms"
          name = "elixir_session_#{i}"

          Postgrex.query!(conn, "SET statement_timeout TO '#{timeout}'", [])
          Postgrex.query!(conn, "SET application_name TO '#{name}'", [])

          for _ <- 1..5 do
            assert Pgdog.one(Postgrex.query!(conn, "SHOW statement_timeout", [])) == timeout

            assert Pgdog.one(
                     Postgrex.query!(conn, "SELECT current_setting($1)", ["application_name"])
                   ) == name
          end
        end)
      end

    Task.await_many(tasks, 30_000)
  end
end
